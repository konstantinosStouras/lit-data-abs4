/*
 * abstracts-ci.mjs — this shard's abstract backfill from OpenAlex + Semantic
 * Scholar + (keyed) Elsevier. VENDORED from the site repo's
 * lit/_scraper-ft50/abstracts-ci.mjs — keep in sync, like the pre-print and
 * citations machinery. Most of this shard's journals deposit no abstract (or a
 * stub) to Crossref — the Elsevier titles especially — so the daily build alone
 * leaves most rows without one.
 * ===========================================================================
 * Many FT50 journals deposit NO abstract (or a stub) to Crossref — finance/
 * econ titles especially — so a large share of data-ft50/ rows render with a
 * missing or teaser abstract. The five INFORMS journals' teasers are fixed by
 * the pubsonline page harvest (lit/data/_informs-abstracts.json); this script
 * covers the ~45 NON-INFORMS journals by DOI over the open APIs:
 *   1. OpenAlex works?filter=doi:<50>&select=doi,abstract_inverted_index
 *      (batched 50/call; the inverted index is reconstructed to text);
 *   2. Semantic Scholar graph/v1/paper/batch?fields=abstract (500 DOIs/POST;
 *      OPTIONAL leg — its anonymous pool 429s freely, so it drops out for the
 *      run while OpenAlex carries on; disable with FT50_ABS_S2=0. An
 *      S2_API_KEY env, when set, is sent as x-api-key so the leg stops
 *      sharing the throttled anonymous pool);
 *   3. Elsevier, for 10.1016/… DOIs — the bulk of the still-missing abstracts
 *      (EJOR, JFE, AOS, OBHDP, JAE, Research Policy, JBV…) are Elsevier
 *      journals whose text neither OpenAlex nor S2 may serve. INERT until an
 *      ELSEVIER_API_KEY secret is set (a free key from dev.elsevier.com) — and
 *      the abstract TEXT needs the institutional token ELSEVIER_INST_TOKEN
 *      beside it: Elsevier entitles abstracts by the caller's institutional
 *      IP range, a GitHub runner is off-campus, and without the token the
 *      Abstract Retrieval API answers 200 with metadata and no dc:description.
 *      The token's terms of use are enforced here: it is a repo SECRET read
 *      from the environment, travels ONLY as the X-ELS-Insttoken header over
 *      https beside the key in X-ELS-APIKey, and is never put in a URL, a log
 *      line or browser code (the selftest pins that). Two legs:
 *      3a. Scopus Search, view=COMPLETE (subscriber-only, hence the token),
 *          25 DOIs per GET as DOI({…}) OR DOI({…}) — the bulk route: 20,000
 *          requests/week per key, so a whole catalog's Elsevier backlog clears
 *          in days instead of months. Drops out for the run on 401/403/429
 *          (a 429 with quota left is the per-second throttle — waited out and
 *          retried once), on a query form Scopus rejects on both spellings
 *          (the exact DOI({…}) form is retried as DOI("…") after a 400 AND
 *          after five chunks of older papers matched nothing; FT50_ABS_SCOPUS_FORM
 *          pins one), or when its X-RateLimit-Remaining hits 0; disable with
 *          FT50_ABS_SCOPUS=0. A record it returns WITHOUT text is a verdict
 *          (Abstract Retrieval reads the same Scopus record); a DOI it does
 *          not return falls through to 3b, or — when 3b is out for the run —
 *          is stamped a PROVISIONAL 7-day miss rather than re-asked of every
 *          leg on every run until the quota resets.
 *      3b. Abstract Retrieval (article/abstract/doi/<doi>?view=META_ABS), one
 *          GET per DOI, paced, for whatever 3a did not serve — 10,000/week
 *          per key. Drops out for the run on 401/403/429 or a spent quota,
 *          naming Elsevier's own error code (AUTHENTICATION_ERROR is a bad
 *          key/token, AUTHORIZATION_ERROR a valid pair without entitlement).
 *          Only a DEFINITIVE answer (200 or 404) counts as "checked"; a 5xx,
 *          a 400 or an unreadable body leaves the DOI uncached for next run.
 *          A 200 WITHOUT text is a tier-2 verdict only once the leg has
 *          retrieved ONE abstract in the run (proof the token unlocks text —
 *          the no-text shape is identical when it does not); before that it
 *          is stamped at tier 1, and 25 of them with no find raises a
 *          ::warning::. A 404 on a paper from the last two years is a 7-day
 *          miss (Scopus indexes an Elsevier article days to weeks after
 *          Crossref registration).
 *      A miss is stamped with the credential TIER it was checked under
 *      (k: 1 = key only, 2 = key + token; absent = the batched legs alone),
 *      and a later run holding a STRONGER credential retries it at once
 *      instead of after the TTL — that is how the tens of thousands of
 *      Elsevier DOIs written off under a refused key are re-checked the day
 *      the token lands, while a DOI the token run confirmed abstract-less is
 *      not re-queried every six hours.
 *   4. Springer Nature Meta API for Springer/Palgrave/Kluwer DOIs, keyed
 *      (SPRINGER_API_KEY), same drop-out rule, tier 1.
 * Results go into data/_api-abstracts.json (doi → {a} |
 * {none:1,t:day[,k:tier][,ttl:days]}); a miss is retried after
 * FT50_ABS_MISS_TTL_DAYS (default 45 — abstracts do get indexed late), after
 * its own shorter ttl where one is stamped, or as soon as a stronger
 * credential is available. The apply step (and the FT50 daily build's
 * applyAbstractCaches) overlays rows UPGRADE-only via betterAbstract — a
 * fuller existing abstract is never replaced.
 *
 * Bounded + resumable: FT50_ABS_BUDGET_MS (default 40 min) per run,
 * FT50_ABS_PACE_MS between OpenAlex calls (default 300 ms, floor 150),
 * newest papers first across the whole catalog; the cache is the resume
 * cursor. Quota identity FT50_ABS_MAILTO (default kstouras+litft50abs@…) so
 * this backfill never starves the citations/preprints identities. The run
 * summary prints the Elsevier X-RateLimit figures it saw (requests left this
 * week per API and when they reset), so the Actions log answers "is the
 * quota the limit?" without a support ticket.
 *
 * Modes: (no args) crawl+apply · --apply-only · --merge-cache <file> ·
 * --dry-run (crawl nothing, report the needy counts).
 * Offline test: node _scraper/abstracts-selftest.mjs
 * ===========================================================================
 */

import { readFile, writeFile, rename } from 'node:fs/promises';
import { existsSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { cleanText, stripPageFurniture, junkAbstract, stripHighlights } from './_entities.mjs';
import { readChunkedJson, writeChunkedJson, chunkPartPath, CHUNK_CAP_BYTES } from './_chunked-json.mjs';

// junkAbstract (user report 2026-08): OpenAlex/S2 mirror the publisher's
// Crossref deposit, so for a paper whose deposit is an editorial plain-language
// summary or a citation-line stub the APIs serve the SAME junk back — each leg
// must reject it against the row's own title/authors or the backfill would
// reinstate exactly what the build guard dropped. (No practitioner-journal
// exemptions in this catalog, unlike the FT50 pipeline's HBR/SMR set.)
const rowGuardCtx = (row) => ({ title: row.Title, authors: row.Authors, journal: row.Journal });

// The shared UPGRADE-only rule + cap, inlined from the site repo's
// lit/_scraper/informs-abstracts.mjs (keep in sync): a candidate replaces the
// current abstract only when the row has none (and the candidate is a real
// paragraph) or when it is MATERIALLY longer — a page fragment can never
// replace fuller existing text.
export const ABS_MAX = 4000;
export function betterAbstract(cur, cand) {
  if (!cand) return false;
  const c = String(cur || '');
  if (!c) return cand.length >= 60;
  return cand.length >= 200 && cand.length > c.length * 1.3;
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.FT50_DATA_DIR || resolve(__dirname, '..', 'data');
const CACHE_PATH = join(DATA_DIR, '_api-abstracts.json');
const MAILTO = process.env.FT50_ABS_MAILTO || 'kstouras+abs4abs@gmail.com';
const BUDGET_MS = parseInt(process.env.FT50_ABS_BUDGET_MS || '', 10) || 40 * 60 * 1000;
const PACE_MS = Math.max(150, parseInt(process.env.FT50_ABS_PACE_MS || '', 10) || 300);
const MISS_TTL_DAYS = parseInt(process.env.FT50_ABS_MISS_TTL_DAYS || '', 10) || 45;
const USE_S2 = process.env.FT50_ABS_S2 !== '0';
// The cache's per-part byte cap. The default (48 MiB) is what keeps a part
// under GitHub's hard 100 MiB push limit; the override exists so the selftest
// can force a split without generating tens of megabytes.
const CHUNK_BYTES = parseInt(process.env.FT50_ABS_CHUNK_BYTES || '', 10) || CHUNK_CAP_BYTES;
// A credential is used as a request HEADER, so it must be a header-safe value:
// printable ASCII, no whitespace or control character inside. A secret pasted
// with an embedded line break would otherwise reach undici's header validator,
// whose error message quotes the offending VALUE — and that message would end
// up in the log. So a malformed one is refused here, by NAME only, and treated
// as unset for the run. Exported for the selftest.
export function headerSafeValue(raw) {
  const v = String(raw == null ? '' : raw).trim();
  return /^[\x21-\x7e]+$/.test(v) ? v : '';
}
function credential(name) {
  const raw = process.env[name];
  if (raw == null || !String(raw).trim()) return '';
  const v = headerSafeValue(raw);
  if (!v) console.log(`::error::${name} is not a valid header value (it contains whitespace or a control character) — re-paste the secret. It is ignored this run.`);
  return v;
}
const S2_KEY = credential('S2_API_KEY');
const ELS_KEY = credential('ELSEVIER_API_KEY');
// Institutional token (X-ELS-Insttoken): Elsevier entitles ABSTRACT text to an
// API key by the caller's INSTITUTIONAL IP RANGE — a GitHub runner is
// off-campus, so without it most Abstract Retrieval responses carry metadata
// but no dc:description, and Scopus's COMPLETE view (the batched route) is
// refused outright. Issued by Elsevier support to the owner for server-side
// use; a repo secret, header-only, inert until set. Its terms: keep it
// server-side, never in browser code, never in a URL, https only, may be
// revoked without notice (a 401 on a run that used to work is that).
const ELS_INSTTOKEN = credential('ELSEVIER_INST_TOKEN');
const ELS_PACE_MS = Math.max(250, parseInt(process.env.FT50_ABS_ELS_PACE_MS || '', 10) || 350);
export const ELS_PREFIX = /^10\.1016\//;
// Scopus Search batch leg (3a): 25 is the page cap of the COMPLETE view, the
// only view that carries dc:description. Paced well under Elsevier's 9 req/s.
const USE_SCOPUS = process.env.FT50_ABS_SCOPUS !== '0';
const SCOPUS_BATCH = 25;
const SCOPUS_PACE_MS = Math.max(250, parseInt(process.env.FT50_ABS_SCOPUS_PACE_MS || '', 10) || 400);
// Scopus answering 200 this many times in a row with NONE of the DOIs asked
// for means the query form or the entitlement is wrong, not that 125 Elsevier
// papers are missing from Scopus — stop spending its quota and let 3b work.
const SCOPUS_EMPTY_STREAK = 5;
// FT50_ABS_SCOPUS_FORM=exact|loose pins the DOI query form; unset = start on
// the exact DOI({…}) form and fall back to DOI("…") on a 400 or an empty streak.
const SCOPUS_FORM_ENV = /^(exact|loose)$/.test((process.env.FT50_ABS_SCOPUS_FORM || '').trim().toLowerCase())
  ? (process.env.FT50_ABS_SCOPUS_FORM || '').trim().toLowerCase() : '';
// A miss that may well change soon is stamped with a SHORT TTL: a 404 on a
// paper from the last two years (Scopus indexes an Elsevier article days to
// weeks after its Crossref registration), and a DOI Scopus did not return
// while the per-DOI leg was out for the run (provisional — nothing decided it).
const SHORT_MISS_TTL_DAYS = 7;
// The per-DOI leg must PROVE the token unlocks text before its no-text answers
// count as tier-2 verdicts: until its first find in the run they are stamped
// at tier 1 (so the next token run re-checks them), and this many of them
// with no find at all raises a ::warning:: — the no-text shape is exactly what
// a token that does not carry the entitlement produces, and a tier-2 stamp
// would be re-opened by nothing.
const NOTEXT_ALARM = 25;
// A 429 that arrives with quota left is Elsevier's per-second throttle (9 req/s
// per KEY, shared by every repository using it) — wait this long and retry once.
const THROTTLE_RETRY_MS = 1500;
// Springer Nature Meta API (free key from dev.springernature.com — the META
// key, not the Open Access one): serves abstracts for Springer/Palgrave/Kluwer
// DOIs. Inert until a SPRINGER_API_KEY secret is set; the leg drops out for
// the run on 401/403/429 so a spent daily quota never stalls the others.
const SPR_KEY = credential('SPRINGER_API_KEY');
const SPR_PACE_MS = Math.max(250, parseInt(process.env.FT50_ABS_SPR_PACE_MS || '', 10) || 400);
export const SPR_PREFIX = /^10\.(1007|1057|1023)\//;
const NEEDY_MAX_LEN = 300; // mirror the INFORMS harvester's teaser threshold
const T0 = Date.now();
const day = () => Math.floor(Date.now() / 86400000);
// The credentials this run holds, in the shape the pure tier/miss rules take.
const CRED = { elsKey: ELS_KEY, elsInsttoken: ELS_INSTTOKEN, sprKey: SPR_KEY,
  elsPrefix: ELS_PREFIX, sprPrefix: SPR_PREFIX };

// Reconstruct plain text from OpenAlex's abstract_inverted_index
// ({word: [positions…]}). Exported for the selftest.
export function invertedToText(inv) {
  if (!inv || typeof inv !== 'object') return '';
  const words = [];
  for (const [w, positions] of Object.entries(inv)) {
    if (!Array.isArray(positions)) continue;
    for (const p of positions) if (Number.isInteger(p) && p >= 0 && p < 100000) words[p] = w;
  }
  return words.filter(x => x !== undefined).join(' ').replace(/\s+/g, ' ').trim();
}

// Should a still-unresolved DOI be written off as a miss for MISS_TTL_DAYS?
// No, if a KEYED per-DOI leg owns this publisher's prefix but never actually
// tried this DOI (bad/expired key, spent quota, or the time budget cut the leg
// mid-batch). Stamping those records a long miss for a check that never
// happened. Pure, so the rule is unit-testable without any network.
export function shouldStampMiss(doi, { elsKey, sprKey, elsPrefix, sprPrefix, keyedTried }) {
  if (elsKey && elsPrefix.test(doi) && !keyedTried.has(doi)) return false;
  if (sprKey && sprPrefix.test(doi) && !keyedTried.has(doi)) return false;
  return true;
}

// The credential TIER this run would check a DOI with: for an Elsevier DOI
// 0 = no key (the batched OpenAlex/S2 legs alone), 1 = key only (metadata,
// no abstract text off-campus), 2 = key + institutional token; for a Springer
// DOI 0 or 1; anything else 0. Pure; exported for the selftest.
export function credentialTier(doi, { elsKey, elsInsttoken, sprKey, elsPrefix, sprPrefix }) {
  if (elsPrefix.test(doi)) return elsKey ? (elsInsttoken ? 2 : 1) : 0;
  if (sprPrefix.test(doi)) return sprKey ? 1 : 0;
  return 0;
}

// Is a cached miss still binding for THIS run? Only when it was stamped under
// a credential at least as strong as the one we hold now (rec.k, absent = 0)
// AND it is younger than its TTL (its own rec.ttl where stamped, else the
// run's). A miss stamped under a weaker credential is retried immediately:
// that is the write-off under a refused key, or the 200-but-no-abstract
// answers a token-less key collects, being re-checked the first run the token
// is available. Pure; exported for the selftest.
export function missIsFresh(rec, { today, ttlDays, tier }) {
  if (!rec || !rec.none) return false;
  if ((rec.k || 0) < (tier || 0)) return false;
  return (today - (rec.t || 0)) < (rec.ttl > 0 ? rec.ttl : ttlDays);
}

// The miss record for a DOI checked under `tier` (k omitted at tier 0 so the
// file stays lean — most of its entries are non-Elsevier DOIs), with its own
// short TTL where the verdict is expected to change soon.
export function missStamp(today, tier, ttl) {
  const rec = tier ? { none: 1, t: today, k: tier } : { none: 1, t: today };
  if (ttl > 0) rec.ttl = ttl;
  return rec;
}

// Did a per-DOI Elsevier answer actually settle the question? 200 (with or
// without text) and 404 (Scopus has no such record) are verdicts. A 5xx, a
// network failure, an unreadable body — and a 400, which is Elsevier's
// INVALID_INPUT, a statement about the request's shape, not about the paper —
// are not: the DOI is left uncached for next run.
export function elsAnswerIsDefinitive(status) {
  return status === 200 || status === 404;
}

// Elsevier's error envelope ({"service-error":{"status":{"statusCode",
// "statusText"}}}) as one line, '' when the body carries none. The HTTP code
// alone does not say what went wrong: a 401 is AUTHENTICATION_ERROR (a bad or
// revoked key/token) OR AUTHORIZATION_ERROR (a valid pair without entitlement
// for the view asked for), and a 429 is QUOTA_EXCEEDED or the per-second
// throttle — the envelope does. Exported for the selftest.
export function elsErrorText(body) {
  const st = body && body['service-error'] && body['service-error'].status;
  if (!st || typeof st !== 'object') return '';
  const code = typeof st.statusCode === 'string' ? st.statusCode.trim() : '';
  const text = typeof st.statusText === 'string' ? st.statusText.trim() : '';
  return [code, text].filter(Boolean).join(': ');
}

// A paper from the current or previous year may simply not be in Scopus yet.
export function isRecentYear(year, nowYear) {
  const y = parseInt(year, 10);
  return Number.isFinite(y) && y >= nowYear - 1;
}

// Scopus advanced-search query for a chunk of DOIs. The 'exact' form wraps
// each DOI in braces — Scopus's exact-match syntax, where the punctuation a
// legacy Elsevier PII DOI carries ("10.1016/S0377-2217(99)00123-4") is taken
// literally; searches are case-insensitive, so our lower-cased bare DOIs
// match the deposited case. The 'loose' form (double quotes) is the fallback
// tried once if Scopus rejects the exact form with a 400.
export function scopusDoiQuery(dois, form = 'exact') {
  return dois.map(d => form === 'loose' ? `DOI("${d}")` : `DOI({${d}})`).join(' OR ');
}

const firstString = (v) => {
  if (typeof v === 'string') return v;
  if (Array.isArray(v)) { for (const x of v) { const s = firstString(x); if (s) return s; } return ''; }
  if (v && typeof v === 'object') { for (const x of Object.values(v)) { const s = firstString(x); if (s) return s; } }
  return '';
};

// Scopus Search JSON → Map(bare lower-cased DOI → cleaned abstract text, '' when
// the record carries none). Shape: {"search-results":{"entry":[{"prism:doi",
// "dc:description",…}]}}; an empty result set is ONE entry {"error":"Result set
// was empty"}, skipped. A DOI listed twice keeps its longer text. Exported for
// the selftest; every text goes through the same page-furniture / highlights
// guards as the other legs.
export function scopusAbstracts(body) {
  const out = new Map();
  const sr = body && body['search-results'];
  const entries = sr && Array.isArray(sr.entry) ? sr.entry : [];
  for (const e of entries) {
    if (!e || typeof e !== 'object' || e.error) continue;
    const doi = String(e['prism:doi'] || '').replace(/^https?:\/\/doi\.org\//i, '').trim().toLowerCase();
    if (!doi) continue;
    const text = stripHighlights(stripPageFurniture(cleanText(firstString(e['dc:description']))));
    if (!out.has(doi) || text.length > out.get(doi).length) out.set(doi, text);
  }
  return out;
}

// What a thrown fetch may say in the log: its name/code, never its message —
// undici's header validator, for one, quotes the offending header VALUE.
const errKind = (e) => (e && (e.code || e.name)) ? String(e.code || e.name) : 'error';

// Elsevier's X-RateLimit-Limit / -Remaining / -Reset headers (reset = epoch
// seconds), or null when the response carries none. Exported for the selftest.
export function readRateLimit(headers) {
  const get = (k) => {
    const v = headers && typeof headers.get === 'function' ? headers.get(k) : null;
    return v == null ? null : String(v).trim();
  };
  const num = (v) => (v != null && /^\d+$/.test(v)) ? parseInt(v, 10) : null;
  const limit = num(get('x-ratelimit-limit'));
  const remaining = num(get('x-ratelimit-remaining'));
  const reset = num(get('x-ratelimit-reset'));
  if (limit == null && remaining == null && reset == null) return null;
  return { limit, remaining, reset };
}

const fmtQuota = (q) => {
  if (!q) return 'no X-RateLimit headers seen';
  const left = q.remaining != null ? `${q.remaining}${q.limit != null ? ` of ${q.limit}` : ''} requests left this week` : 'remaining unknown';
  const reset = q.reset != null ? `, resets ${new Date(q.reset * 1000).toISOString().slice(0, 16).replace('T', ' ')} UTC` : '';
  return left + reset;
};

// The abstract text out of Elsevier's Abstract Retrieval JSON. Exported for
// the selftest. The shape is {"abstracts-retrieval-response":{"coredata":
// {"dc:description": …}}}; dc:description is usually a plain string but can be
// a nested {abstract:{'ce:para': …}} object — take the first string found.
export function elsevierAbstract(body) {
  const core = body && body['abstracts-retrieval-response'] &&
    body['abstracts-retrieval-response'].coredata;
  const d = core && core['dc:description'];
  // stripPageFurniture (feedback LIT-260727-XRQ8): reject a scraped
  // article-page blob served in place of abstract prose.
  return stripHighlights(stripPageFurniture(cleanText(firstString(d))));
}

// The abstract out of a Springer Meta API v2 JSON response ({records:[{abstract}]}).
// Exported for the selftest; tolerant of the abstract arriving as a nested
// object the way elsevierAbstract is.
export function springerAbstract(body) {
  const rec = body && Array.isArray(body.records) && body.records[0];
  return stripHighlights(stripPageFurniture(cleanText(firstString(rec && rec.abstract)).replace(/^Abstract\s+/i, '')));
}

// Merge another cache in: an entry WITH an abstract beats a none-record, a
// longer abstract beats a shorter one, and between two none-records the one
// stamped under the STRONGER credential (then the newer) wins — so the
// push-retry replay never hides a token-checked verdict behind an older
// keyless write-off. No direction downgrades an abstract. Exported for the
// selftest and used by --merge-cache (CI push-retry replay).
export function mergeAbsCache(cache, other) {
  let took = 0;
  for (const [k, v] of Object.entries(other || {})) {
    if (!v || typeof v !== 'object') continue;
    const cur = cache[k];
    const strongerMiss = cur && cur.none && v.none &&
      ((v.k || 0) > (cur.k || 0) || ((v.k || 0) === (cur.k || 0) && (v.t || 0) > (cur.t || 0)));
    if (!cur || (v.a && (!cur.a || v.a.length > cur.a.length)) || strongerMiss) { cache[k] = v; took++; }
  }
  return took;
}

// A row needs an abstract when it's missing or a sub-300-char stub. Exported
// for the selftest.
export function isNeedy(row) {
  return !row.Abstract || row.Abstract.length < NEEDY_MAX_LEN;
}

const bareDoi = (row) => (row.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();

async function awrite(dest, str) {
  const tmp = `${dest}.tmp-${process.pid}`;
  await writeFile(tmp, str, 'utf8');
  try { await rename(tmp, dest); } catch { await writeFile(dest, str, 'utf8'); }
}

async function loadJson(path, fallback) {
  if (!existsSync(path)) return fallback;
  try { return JSON.parse(await readFile(path, 'utf8')); } catch { return fallback; }
}

function paperFiles() {
  return readdirSync(DATA_DIR).filter(f => /^papers-[a-z0-9]+\.json$/.test(f)).sort();
}

// Exported so the selftest can drive a whole run against a stubbed fetch.
export async function main() {
  const args = process.argv.slice(2);
  const APPLY_ONLY = args.includes('--apply-only');
  const DRY = args.includes('--dry-run');
  const MERGE = (() => {
    const eq = args.find(a => a.startsWith('--merge-cache='));
    if (eq) return eq.slice('--merge-cache='.length);
    const i = args.indexOf('--merge-cache');
    return i >= 0 ? String(args[i + 1] || '') : '';
  })();

  // The cache is CHUNKED (see _chunked-json.mjs): a cache that outgrew
  // GitHub's hard 100 MiB push limit is what stopped lit-data-abs3-omecon
  // committing anything for three runs (102.41 MB, rejected by the
  // pre-receive hook while every run did its work and threw it away). Both
  // sides read through readChunkedJson, so a single-file cache written before
  // this — and a merge copy of one — still loads exactly as it did.
  const rawCache = await readChunkedJson(CACHE_PATH, {});
  const cache = rawCache.map || rawCache;
  if (MERGE) {
    const rawMerge = await readChunkedJson(MERGE, {});
    const took = mergeAbsCache(cache, rawMerge.map || rawMerge);
    console.log(`  merge-cache: took ${took} entries from ${MERGE}`);
  }

  // Heal any furniture-contaminated entry an old run cached (or a stale
  // merged cache brings back) — feedback LIT-260727-XRQ8: Semantic Scholar
  // sometimes serves a scrape of the whole article page for items with no
  // abstract. A cut that leaves a real abstract keeps it; a chrome-only
  // entry is re-stamped a miss so the TTL retry re-checks it under the guard.
  {
    let healed = 0;
    for (const [k, v] of Object.entries(cache)) {
      if (!v || !v.a) continue;
      const t = stripHighlights(stripPageFurniture(v.a));
      if (t === v.a) continue;
      if (t.length >= 60) cache[k] = { a: t }; else cache[k] = { none: 1, t: day() };
      healed++;
    }
    if (healed) console.log(`  healed ${healed} furniture/highlights-contaminated cached abstracts`);
  }

  // Written in parts, each well under the 100 MiB push limit; a shrinking
  // rewrite deletes the leftovers, so a part can never resurrect old entries.
  let cacheParts = 1;
  async function saveCache() {
    const sorted = {};
    for (const k of Object.keys(cache).sort()) sorted[k] = cache[k];
    cacheParts = (await writeChunkedJson(CACHE_PATH, sorted, CHUNK_BYTES)).length;
  }

  async function applyToPapers() {
    let total = 0, junked = 0;
    for (const f of paperFiles()) {
      const p = join(DATA_DIR, f);
      const rows = await loadJson(p, null);
      if (!rows) continue;
      let up = 0;
      for (const row of rows) {
        const doi = bareDoi(row);
        const rec = cache[doi];
        if (!rec || !rec.a) continue;
        // A junk "abstract" cached before the guard existed (an API copy of a
        // publisher summary/citation-stub deposit) is never applied and is
        // re-stamped a TTL miss so it is re-resolved under the guard.
        const ctx = rowGuardCtx(row);
        if (ctx && junkAbstract(rec.a, ctx)) {
          cache[doi] = { none: 1, t: day() };
          junked++;
          continue;
        }
        if (betterAbstract(row.Abstract, rec.a)) { row.Abstract = rec.a.slice(0, ABS_MAX); up++; }
      }
      if (up) { await awrite(p, JSON.stringify(rows)); total += up; console.log(`  ${f}: upgraded ${up} abstracts`); }
    }
    if (junked) { console.log(`  re-stamped ${junked} junk cached "abstracts" as misses`); await saveCache(); }
    console.log(total
      ? `✓ Applied the API cache to the served papers files (${total} abstracts).`
      : '  Papers files already carry every cached abstract — nothing to apply.');
  }

  if (APPLY_ONLY) {
    if (MERGE) await saveCache();
    await applyToPapers();
    return;
  }

  // Needy list: every FT50 row with a missing/stub abstract, newest first,
  // skipping cached hits and misses that are still binding for the credential
  // this run holds (missIsFresh). rowMeta carries each needy row's
  // title/authors/journal so the API legs can reject a junk "abstract" (a
  // mirror of the publisher's summary/citation-stub deposit) on arrival.
  const needy = [];
  const rowMeta = new Map();
  let needyEls = 0, retriedWeaker = 0;
  for (const f of paperFiles()) {
    const rows = await loadJson(join(DATA_DIR, f), []);
    for (const row of rows) {
      if (!isNeedy(row)) continue;
      const doi = bareDoi(row);
      if (!doi) continue;
      const ctx = rowGuardCtx(row);
      if (ctx && !rowMeta.has(doi)) rowMeta.set(doi, ctx);
      const cur = cache[doi];
      if (cur && cur.a) continue;
      const tier = credentialTier(doi, CRED);
      if (missIsFresh(cur, { today: day(), ttlDays: MISS_TTL_DAYS, tier })) continue;
      if (cur && cur.none && (cur.k || 0) < tier && (day() - (cur.t || 0)) < MISS_TTL_DAYS) retriedWeaker++;
      if (ELS_PREFIX.test(doi)) needyEls++;
      needy.push({ doi, y: parseInt(row.Year, 10) || 0 });
    }
  }
  const junkForDoi = (doi, text) => {
    const m = rowMeta.get(doi);
    return m ? junkAbstract(text, m) : '';
  };
  needy.sort((a, b) => b.y - a.y);
  const yearOf = new Map(needy.map(n => [n.doi, n.y]));
  const NOW_YEAR = new Date().getUTCFullYear();
  console.log(`${needy.length} papers need an abstract (missing/stub, cache-eligible)` +
    `${needyEls ? `; ${needyEls} of them Elsevier DOIs` : ''}` +
    `${retriedWeaker ? `, ${retriedWeaker} unexpired misses re-eligible under this run's stronger credential` : ''}.`);
  if (ELS_KEY && !ELS_INSTTOKEN && needyEls) {
    console.log('  ELSEVIER_API_KEY is set without ELSEVIER_INST_TOKEN: off-campus, Elsevier serves metadata but ' +
      'no abstract text to a bare key and the batched Scopus leg is off — set the institutional token secret.');
  }
  if (ELS_INSTTOKEN && !ELS_KEY) {
    console.log('::warning::ELSEVIER_INST_TOKEN is set but ELSEVIER_API_KEY is not — the token pairs with a key ' +
      'and does nothing alone, so both Elsevier legs are off this run. Add the ELSEVIER_API_KEY secret.');
  }
  if (DRY) return;

  let s2ok = USE_S2, elsOk = true, found = 0, checked = 0, batches = 0;
  // Why the per-DOI leg stopped, when it did: 'refused' (401/403), 'quota'
  // (429 or Remaining: 0 — back after the weekly reset) or 'network' (a thrown
  // fetch — may well be back next run, so nothing is stamped on its account).
  let elsDropCode = 0, elsDropWhy = '', elsDropText = '', elsDropKind = '';
  // Elsevier DOIs no Elsevier leg reached this run: left uncached on purpose
  // (a check that never happened is never written off), so they come back.
  let undecidedEls = 0;
  const elsStats = { found: 0, empty: 0, e404: 0, other: 0, transient: 0 };
  let scopusOk = USE_SCOPUS && !!ELS_KEY && !!ELS_INSTTOKEN, scopusDrop = '', scopusForm = SCOPUS_FORM_ENV || 'exact', scopusEmptyStreak = 0, scopus400s = 0;
  const scopusActive = scopusOk;
  const scopusStats = { calls: 0, found: 0, noText: 0, unmatched: 0, provisional: 0 };
  const quota = { scopus: null, abstract: null };
  const noteQuota = (which, r) => { const q = readRateLimit(r.headers); if (q) quota[which] = q; return q; };
  // Elsevier's error envelope off a refusal, without letting a bad body throw.
  const errText = async (r) => { try { return elsErrorText(await r.json()); } catch { return ''; } };
  // The per-DOI leg's no-text answers this run, and whether it has proved the
  // token unlocks text (one real abstract retrieved) — see NOTEXT_ALARM.
  const noTextThisRun = new Set();
  const elsProven = () => elsStats.found > 0;
  // The key in X-ELS-APIKey and the token in X-ELS-Insttoken — headers only.
  const elsHeaders = () => ({ 'X-ELS-APIKey': ELS_KEY, Accept: 'application/json',
    ...(ELS_INSTTOKEN ? { 'X-ELS-Insttoken': ELS_INSTTOKEN } : {}) });
  let sprOk = true;
  const sprStats = { found: 0, empty: 0, other: 0 };
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  for (let i = 0; i < needy.length; i += 50) {
    if (Date.now() - T0 > BUDGET_MS) { console.log('⏱ budget spent — stopping this slice (resume-safe).'); break; }
    const batch = needy.slice(i, i + 50).map(n => n.doi);
    let unresolved = new Set(batch);
    // Which DOIs a KEYED, per-DOI leg actually got a verdict on this batch. A
    // keyed leg can end early — a bad/expired key or spent quota drops it for
    // the whole run (401/403/429), and the time budget can cut it mid-batch —
    // and a DOI it never reached must NOT be stamped as a miss below.
    const keyedTried = new Set();
    // Per-DOI stamp overrides decided by the legs: a short TTL for a verdict
    // that may change soon (a recent paper's 404, a provisional Scopus miss).
    const stampTtl = new Map();
    // Leg 1: OpenAlex
    try {
      const url = `https://api.openalex.org/works?filter=doi:${batch.join('|')}` +
        `&per-page=50&select=doi,abstract_inverted_index&mailto=${MAILTO}`;
      const r = await fetch(url, { headers: { 'User-Agent': `lit-ft50-abstracts/1.0 (mailto:${MAILTO})` } });
      if (r.status === 429 || r.status === 403) { console.log(`OpenAlex throttled (HTTP ${r.status}) — ending the run cleanly.`); break; }
      if (r.ok) {
        for (const w of (await r.json()).results || []) {
          const doi = String(w.doi || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
          const text = stripHighlights(stripPageFurniture(cleanText(invertedToText(w.abstract_inverted_index))));
          if (doi && text.length >= 60 && !junkForDoi(doi, text)) { cache[doi] = { a: text.slice(0, ABS_MAX) }; unresolved.delete(doi); found++; }
        }
      }
    } catch (e) { console.warn(`  OpenAlex batch failed: ${errKind(e)}`); }
    // Leg 2: Semantic Scholar (optional; drops out on throttle)
    if (s2ok && unresolved.size) {
      try {
        const r = await fetch(`https://api.semanticscholar.org/graph/v1/paper/batch?fields=abstract`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...(S2_KEY ? { 'x-api-key': S2_KEY } : {}) },
          body: JSON.stringify({ ids: [...unresolved].map(d => `DOI:${d}`) }),
        });
        if (r.status === 429) { s2ok = false; console.log('  Semantic Scholar throttled — dropping that leg for this run.'); }
        else if (r.ok) {
          const ids = [...unresolved];
          const arr = await r.json();
          arr.forEach((rec, idx) => {
            const a = rec && typeof rec.abstract === 'string' ? stripHighlights(stripPageFurniture(cleanText(rec.abstract))) : '';
            if (a.length >= 60 && !junkForDoi(ids[idx], a)) { cache[ids[idx]] = { a: a.slice(0, ABS_MAX) }; unresolved.delete(ids[idx]); found++; }
          });
        }
      } catch (e) { s2ok = false; console.warn(`  Semantic Scholar leg failed (${errKind(e)}) — dropping it for this run.`); }
    }
    // Leg 3a: Scopus Search, batched (keyed + token; Elsevier DOIs only). A
    // record it returns WITHOUT text is a verdict (Abstract Retrieval reads the
    // same Scopus record) and joins keyedTried; a DOI it does not return goes
    // on to leg 3b, and is remembered in scopusMissed for the provisional stamp
    // below should 3b be out for the run.
    const scopusMissed = new Set();
    if (scopusOk && unresolved.size) {
      const elsDois = [...unresolved].filter(d => ELS_PREFIX.test(d));
      for (let j = 0; j < elsDois.length && scopusOk; j += SCOPUS_BATCH) {
        if (Date.now() - T0 > BUDGET_MS) break;
        const chunk = elsDois.slice(j, j + SCOPUS_BATCH);
        // Papers from the last two years may simply not be in Scopus yet, so a
        // chunk made only of them says nothing about the query form.
        const chunkIsAllRecent = chunk.every(d => isRecentYear(yearOf.get(d), NOW_YEAR));
        let settled = false, retried400 = false, retriedStreak = false, retried429 = false;
        while (!settled && scopusOk) {
          settled = true;
          let r;
          try {
            r = await fetch(
              `https://api.elsevier.com/content/search/scopus?query=${encodeURIComponent(scopusDoiQuery(chunk, scopusForm))}` +
              `&view=COMPLETE&count=${SCOPUS_BATCH}&httpAccept=application/json`,
              { headers: elsHeaders() });
          } catch (e) { scopusOk = false; scopusDrop = `network error (${errKind(e)})`; break; }
          scopusStats.calls++;
          const q = noteQuota('scopus', r);
          if (r.status === 429 && q && q.remaining > 0 && !retried429) {
            // Quota left: the per-second throttle, not the week's quota.
            retried429 = true; settled = false; await sleep(THROTTLE_RETRY_MS); continue;
          }
          if (r.status === 401 || r.status === 403 || r.status === 429) {
            const why = await errText(r);
            scopusOk = false; scopusDrop = `HTTP ${r.status}${why ? ` (${why})` : ''}`; break;
          }
          if (r.status === 400) {
            if (scopusForm === 'exact' && !SCOPUS_FORM_ENV && !retried400) {
              retried400 = true; scopusForm = 'loose'; settled = false;
              console.log('  Scopus rejected the DOI({…}) query form (HTTP 400) — retrying this chunk as DOI("…").');
              await sleep(SCOPUS_PACE_MS); continue;
            }
            // No form left to try for THIS chunk (one un-quotable DOI is enough):
            // its DOIs go on to leg 3b and the next chunk is tried; three such
            // chunks in a run mean the query itself is wrong.
            if (++scopus400s >= 3) { scopusOk = false; scopusDrop = `HTTP 400 on ${scopus400s} chunks (${scopusForm} form)`; }
            break;
          }
          if (!r.ok) break; // transient (5xx…): this chunk goes to leg 3b, next chunk tries again
          let map;
          try { map = scopusAbstracts(await r.json()); } catch { break; }
          let matched = 0;
          for (const doi of chunk) {
            if (!map.has(doi)) { scopusStats.unmatched++; continue; }
            matched++;
            const text = map.get(doi);
            if (text.length >= 60 && !junkForDoi(doi, text)) {
              cache[doi] = { a: text.slice(0, ABS_MAX) }; unresolved.delete(doi); found++; scopusStats.found++;
            } else { scopusStats.noText++; keyedTried.add(doi); } // the record carries no abstract: a verdict
          }
          scopusEmptyStreak = (matched || chunkIsAllRecent) ? 0 : scopusEmptyStreak + 1;
          if (scopusEmptyStreak >= SCOPUS_EMPTY_STREAK) {
            if (scopusForm === 'exact' && !SCOPUS_FORM_ENV && !retriedStreak) {
              // Scopus accepted the exact form yet matched nothing for five chunks
              // of older papers: try the quoted form on this chunk before giving up.
              retriedStreak = true; scopusForm = 'loose'; scopusEmptyStreak = 0; settled = false;
              console.log(`  Scopus answered ${SCOPUS_EMPTY_STREAK} chunks with none of the DOIs asked for — retrying this chunk as DOI("…").`);
              await sleep(SCOPUS_PACE_MS); continue;
            }
            scopusOk = false; scopusDrop = `${SCOPUS_EMPTY_STREAK} consecutive answers matched none of the DOIs asked for (${scopusForm} form)`;
          }
          if (q && q.remaining === 0) { scopusOk = false; scopusDrop = 'weekly quota spent (X-RateLimit-Remaining: 0)'; }
          for (const doi of chunk) if (unresolved.has(doi) && !keyedTried.has(doi)) scopusMissed.add(doi);
        }
        await sleep(SCOPUS_PACE_MS);
      }
      if (!scopusOk && scopusDrop) console.log(`  Scopus leg dropped for this run (${scopusDrop}) — Elsevier DOIs continue on the per-DOI leg.`);
    }
    // Leg 3b: Elsevier Abstract Retrieval (keyed; Elsevier DOIs only). One GET
    // per DOI — the leg drops out for the run on 401/403/429 or a spent quota
    // so it can never stall the batched legs.
    if (elsOk && ELS_KEY && unresolved.size) {
      for (const doi of [...unresolved]) {
        if (!ELS_PREFIX.test(doi) || keyedTried.has(doi)) continue;
        if (Date.now() - T0 > BUDGET_MS) break;
        try {
          // view=META_ABS is what includes dc:description — the default view
          // returns metadata WITHOUT the abstract text. The DOI travels with
          // its slash literal (each segment encoded), Elsevier's documented form.
          const url = `https://api.elsevier.com/content/abstract/doi/${doi.split('/').map(encodeURIComponent).join('/')}` +
            '?view=META_ABS&httpAccept=application/json';
          let r = await fetch(url, { headers: elsHeaders() });
          let q = noteQuota('abstract', r);
          if (r.status === 429 && q && q.remaining > 0) {
            // Quota left: the per-second throttle, not the week's quota — wait and retry once.
            await sleep(THROTTLE_RETRY_MS);
            r = await fetch(url, { headers: elsHeaders() });
            q = noteQuota('abstract', r);
          }
          if (r.status === 401 || r.status === 403 || r.status === 429) {
            elsDropText = await errText(r);
            elsOk = false; elsDropCode = r.status; elsDropWhy = `HTTP ${r.status}${elsDropText ? ` (${elsDropText})` : ''}`;
            elsDropKind = r.status === 429 ? 'quota' : 'refused';
            console.log(`  Elsevier leg dropped for this run (${elsDropWhy}).`); break;
          }
          if (r.ok) {
            let body = null, readable = true;
            try { body = await r.json(); } catch { readable = false; }
            if (!readable) elsStats.transient++; // an unreadable body is not a verdict
            else {
              const text = elsevierAbstract(body);
              keyedTried.add(doi);
              if (text.length >= 60 && !junkForDoi(doi, text)) { cache[doi] = { a: text.slice(0, ABS_MAX) }; unresolved.delete(doi); found++; elsStats.found++; }
              else { elsStats.empty++; noTextThisRun.add(doi); }
            }
          } else if (r.status === 404) {
            keyedTried.add(doi); elsStats.e404++;
            if (isRecentYear(yearOf.get(doi), NOW_YEAR)) stampTtl.set(doi, SHORT_MISS_TTL_DAYS);
          } else if (r.status >= 500) elsStats.transient++;
          else elsStats.other++; // 400 and the like: about the request, not the paper — left uncached
          if (q && q.remaining === 0) {
            elsOk = false; elsDropWhy = 'weekly quota spent (X-RateLimit-Remaining: 0)'; elsDropKind = 'quota';
            console.log(`  Elsevier leg dropped for this run (${elsDropWhy}).`); break;
          }
        } catch (e) { elsOk = false; elsDropWhy = `network error (${errKind(e)})`; elsDropKind = 'network'; console.warn(`  Elsevier leg failed (${errKind(e)}) — dropping it for this run.`); break; }
        await sleep(ELS_PACE_MS);
      }
    }
    // Leg 4: Springer Nature Meta API (keyed; Springer/Palgrave/Kluwer DOIs).
    if (sprOk && SPR_KEY && unresolved.size) {
      for (const doi of [...unresolved]) {
        if (!SPR_PREFIX.test(doi)) continue;
        if (Date.now() - T0 > BUDGET_MS) break;
        try {
          const r = await fetch(
            `https://api.springernature.com/meta/v2/json?q=doi:%22${encodeURIComponent(doi)}%22&p=1&api_key=${encodeURIComponent(SPR_KEY)}`,
            { headers: { Accept: 'application/json' } });
          if (r.status === 401 || r.status === 403 || r.status === 429) {
            sprOk = false; console.log(`  Springer leg dropped for this run (HTTP ${r.status} — key/quota).`); break;
          }
          if (r.status < 500) keyedTried.add(doi); // a 5xx is not a verdict
          if (r.ok) {
            const text = springerAbstract(await r.json());
            if (text.length >= 60 && !junkForDoi(doi, text)) { cache[doi] = { a: text.slice(0, ABS_MAX) }; unresolved.delete(doi); found++; sprStats.found++; }
            else sprStats.empty++;
          } else sprStats.other++;
        } catch (e) { sprOk = false; console.warn(`  Springer leg failed (${errKind(e)}) — dropping it for this run.`); break; }
        await sleep(SPR_PACE_MS);
      }
    }
    // All legs concluded for this batch: stamp the rest as misses (TTL-retried,
    // carrying the credential tier they were checked under). EXCEPT a DOI whose
    // keyed leg never actually tried it. Stamping those records a 45-day miss
    // for a check that never happened — which is exactly what a rejected
    // Elsevier key produced: the leg dropped on its first call, yet ~17k EJOR
    // DOIs were written off as "no abstract available" in a 9-minute run that
    // could not physically have queried them (one GET per DOI at ELS_PACE_MS
    // would have taken over an hour). Leave them uncached so the next run
    // retries them for real.
    // DOIs Scopus did not return while the per-DOI leg was out for the run on
    // a spent quota or a refusal: nothing decided them, so they are stamped
    // PROVISIONALLY (7 days) rather than re-sent to OpenAlex, S2 and Scopus on
    // every run until the quota resets — a cost that would eat the Scopus
    // quota on questions already asked. A leg lost to a network error may be
    // back next run, so that case stamps nothing; with the leg up they were
    // its to decide, as above.
    const provisional = new Set();
    if (!elsOk && elsDropKind && elsDropKind !== 'network') {
      for (const doi of scopusMissed) {
        if (!unresolved.has(doi) || keyedTried.has(doi)) continue;
        provisional.add(doi); stampTtl.set(doi, SHORT_MISS_TTL_DAYS); scopusStats.provisional++;
      }
    }
    for (const doi of unresolved) {
      if (!provisional.has(doi) && !shouldStampMiss(doi, { elsKey: ELS_KEY, sprKey: SPR_KEY,
        elsPrefix: ELS_PREFIX, sprPrefix: SPR_PREFIX, keyedTried })) {
        if (ELS_PREFIX.test(doi)) undecidedEls++;
        continue;
      }
      let tier = credentialTier(doi, CRED);
      // A no-text answer from a token run that retrieved NOTHING is not yet a
      // tier-2 verdict: the shape is identical when the token does not carry
      // the entitlement, and a tier-2 stamp would be re-opened by nothing.
      // Tier 1 keeps it re-checkable by the next token run.
      if (tier === 2 && noTextThisRun.has(doi) && !elsProven()) tier = 1;
      cache[doi] = missStamp(day(), tier, stampTtl.get(doi));
    }
    checked += batch.length;
    if (++batches % 5 === 0) { await saveCache(); console.log(`  …${checked} DOIs checked, ${found} abstracts found`); }
    await sleep(PACE_MS);
  }

  await saveCache();
  const withA = Object.values(cache).filter(v => v && v.a).length;
  console.log(`\n✓ Wrote ${CACHE_PATH}` + (cacheParts > 1 ? ` (+ ${cacheParts - 1} more part${cacheParts > 2 ? 's' : ''}, through ${chunkPartPath(CACHE_PATH, cacheParts)})` : ''));
  console.log(`  This run: ${checked} DOIs checked, ${found} abstracts found.`);
  if (SPR_KEY && (sprStats.found + sprStats.empty + sprStats.other)) {
    console.log(`  Springer leg: ${sprStats.found} found, ${sprStats.empty} no-abstract, ${sprStats.other} other.`);
  }
  if (scopusActive) {
    console.log(`  Scopus leg: ${scopusStats.calls} queries (${scopusForm} DOI form), ${scopusStats.found} found, ` +
      `${scopusStats.noText} matched without text (verdicts), ${scopusStats.unmatched} not returned` +
      (scopusStats.provisional ? ` (${scopusStats.provisional} of them stamped provisional 7-day misses while the per-DOI leg was out)` : '') +
      (scopusDrop ? ` — dropped: ${scopusDrop}` : '') + '.');
    if (scopusDrop && /^HTTP 40[13]/.test(scopusDrop)) {
      console.log(`::warning::Scopus Search refused the COMPLETE view (${scopusDrop}) — AUTHORIZATION_ERROR means the ` +
        'institutional token does not carry a Scopus subscription for this account, AUTHENTICATION_ERROR that the ' +
        'key/token pair is wrong. The per-DOI Abstract Retrieval leg still runs (10,000 requests/week), just slower.');
    }
  }
  // A keyed leg that was configured but achieved nothing is the difference
  // between "this publisher has no abstracts" and "our credential is being
  // refused" — and the run otherwise exits 0 and looks healthy either way.
  // Say so loudly: ::warning:: surfaces it on the Actions run page.
  if (ELS_KEY && (elsDropCode === 401 || elsDropCode === 403)) {
    const said = elsDropText ? ` Elsevier said: ${elsDropText}.` : '';
    const meaning = /AUTHENTICATION/i.test(elsDropText)
      ? 'AUTHENTICATION_ERROR is a bad, expired or revoked key/token (Elsevier may revoke the institutional token without notice).'
      : /AUTHORIZATION/i.test(elsDropText)
        ? 'AUTHORIZATION_ERROR is a VALID key/token pair without entitlement for what was asked — ask Elsevier support which entitlements the token carries; do not rotate the credentials.'
        : (ELS_INSTTOKEN
          ? 'With ELSEVIER_INST_TOKEN set this is either a bad/revoked key or token (AUTHENTICATION_ERROR) or a valid pair without entitlement (AUTHORIZATION_ERROR) — the error code above says which.'
          : 'A bare key lacks abstract entitlement off-campus — request an institutional token via dev.elsevier.com support and set ELSEVIER_INST_TOKEN.');
    console.log(`::warning::Elsevier refused the request (HTTP ${elsDropCode}); the Elsevier per-DOI leg did no work this run.${said} ${meaning} ` +
      'Elsevier journals (EJOR, JFE, AOS, OBHDP, JAE, Research Policy…) are the bulk of the still-missing abstracts, so this is why their coverage is not moving.');
  } else if (ELS_KEY && elsDropCode === 429 && quota.abstract && quota.abstract.remaining > 0) {
    console.log(`::notice::Elsevier throttled the per-DOI leg (HTTP 429 with quota left${elsDropText ? `: ${elsDropText}` : ''}); the 9 requests/second ` +
      'limit is per KEY, shared by every repository using it, and a retry after a pause was throttled too — the leg stopped for this run and resumes next run. ' +
      `Quota: ${fmtQuota(quota.abstract)}.`);
  } else if (ELS_KEY && (elsDropCode === 429 || /quota/.test(elsDropWhy))) {
    console.log(`::notice::Elsevier's weekly Abstract Retrieval quota is spent (${elsDropWhy}); the leg resumes on the next run after the reset. ` +
      `Quota: ${fmtQuota(quota.abstract)}.`);
  }
  if (SPR_KEY && sprOk === false) {
    console.log('::warning::SPRINGER_API_KEY is set but the Springer leg dropped out this run.');
  }
  if (ELS_KEY && (elsStats.found + elsStats.empty + elsStats.e404 + elsStats.other + elsStats.transient)) {
    console.log(`  Elsevier per-DOI leg: ${elsStats.found} found, ${elsStats.empty} 200-but-no-abstract, ` +
      `${elsStats.e404} not-found, ${elsStats.other} other 4xx, ${elsStats.transient} transient (left for next run).`);
    if (ELS_INSTTOKEN && elsStats.found === 0 && elsStats.empty >= NOTEXT_ALARM) {
      console.log(`::warning::The institutional token is not unlocking abstract text: ${elsStats.empty} Elsevier answers carried ` +
        'no abstract and none carried one. Those DOIs were stamped at tier 1 (re-checked by the next token run), nothing was ' +
        'written off. Ask Elsevier support to confirm the token\'s entitlements for the Abstract Retrieval API (META_ABS view).');
    } else if (elsStats.empty > 20 && elsStats.found < elsStats.empty / 10) {
      const why = ELS_INSTTOKEN
        ? 'the institutional token is unlocking little text — ask Elsevier support to confirm its entitlements.'
        : 'the key is likely missing off-campus ABSTRACT entitlement — request an institutional token (X-ELS-Insttoken) via dev.elsevier.com support and set ELSEVIER_INST_TOKEN.';
      console.log(`  ↳ mostly empty responses: ${why}`);
    }
  }
  if (ELS_KEY && (quota.scopus || quota.abstract)) {
    console.log(`  Elsevier quota — Scopus Search: ${fmtQuota(quota.scopus)}; Abstract Retrieval: ${fmtQuota(quota.abstract)}.`);
  }
  if (undecidedEls) {
    console.log(`::notice::${undecidedEls} Elsevier DOIs were left uncached this run: no Elsevier leg reached them, and a ` +
      'check that never happened is never written off. They stay in the needy queue, so every run re-asks the batched ' +
      'OpenAlex/Semantic Scholar legs about them until an Elsevier leg can answer — fix the credential above and that stops.');
  }
  console.log(`  Cache now maps ${Object.keys(cache).length} DOIs (${withA} with abstracts).`);
  await applyToPapers();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(e => { console.error(e); process.exit(1); });
}
