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
 *   3. Elsevier Abstract Retrieval API (article/abstract/doi/<doi>) for
 *      10.1016/… DOIs — the bulk of the still-missing abstracts (EJOR, JFE,
 *      AOS, OBHDP, JAE, Research Policy, JBV…) are Elsevier journals whose
 *      text neither OpenAlex nor S2 may serve. INERT until an
 *      ELSEVIER_API_KEY env/secret is set (a free institutional key from
 *      dev.elsevier.com); one GET per DOI, paced, and the leg drops out for
 *      the run on 401/403/429 (quota) so it can never stall the others. When
 *      the key is present, Elsevier-prefix MISSES are retried immediately
 *      (their earlier {none} stamps came from the keyless runs).
 * Results go into data-ft50/_api-abstracts.json (doi → {a} | {none:1,t:day});
 * a miss is retried after FT50_ABS_MISS_TTL_DAYS (default 45 — abstracts do
 * get indexed late). The apply step (and the FT50 daily build's
 * applyAbstractCaches) overlays rows UPGRADE-only via betterAbstract — a
 * fuller existing abstract is never replaced.
 *
 * Bounded + resumable: FT50_ABS_BUDGET_MS (default 40 min) per run,
 * FT50_ABS_PACE_MS between OpenAlex calls (default 300 ms, floor 150),
 * newest papers first across the whole catalog; the cache is the resume
 * cursor. Quota identity FT50_ABS_MAILTO (default kstouras+litft50abs@…) so
 * this backfill never starves the citations/preprints identities.
 *
 * Modes: (no args) crawl+apply · --apply-only · --merge-cache <file> ·
 * --dry-run (crawl nothing, report the needy counts).
 * Offline test: node lit/_scraper-ft50/abstracts-selftest.mjs
 * ===========================================================================
 */

import { readFile, writeFile, rename } from 'node:fs/promises';
import { existsSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { cleanText } from './_entities.mjs';

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
const S2_KEY = process.env.S2_API_KEY || '';
const ELS_KEY = process.env.ELSEVIER_API_KEY || '';
// Optional institutional token (X-ELS-Insttoken): Elsevier entitles ABSTRACT
// text to an API key by the caller's INSTITUTIONAL IP RANGE — a GitHub runner
// is off-campus, so without an insttoken most responses can carry metadata but
// no dc:description. Request one via dev.elsevier.com support for server-side
// use; inert until set.
const ELS_INSTTOKEN = process.env.ELSEVIER_INST_TOKEN || '';
const ELS_PACE_MS = Math.max(250, parseInt(process.env.FT50_ABS_ELS_PACE_MS || '', 10) || 350);
const ELS_PREFIX = /^10\.1016\//;
const NEEDY_MAX_LEN = 300; // mirror the INFORMS harvester's teaser threshold
const T0 = Date.now();
const day = () => Math.floor(Date.now() / 86400000);

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

// The abstract text out of Elsevier's Abstract Retrieval JSON. Exported for
// the selftest. The shape is {"abstracts-retrieval-response":{"coredata":
// {"dc:description": …}}}; dc:description is usually a plain string but can be
// a nested {abstract:{'ce:para': …}} object — take the first string found.
export function elsevierAbstract(body) {
  const core = body && body['abstracts-retrieval-response'] &&
    body['abstracts-retrieval-response'].coredata;
  const d = core && core['dc:description'];
  const firstString = (v) => {
    if (typeof v === 'string') return v;
    if (Array.isArray(v)) { for (const x of v) { const s = firstString(x); if (s) return s; } return ''; }
    if (v && typeof v === 'object') { for (const x of Object.values(v)) { const s = firstString(x); if (s) return s; } }
    return '';
  };
  return cleanText(firstString(d));
}

// Merge another cache in: an entry WITH an abstract beats a none-record, a
// longer abstract beats a shorter one; no direction downgrades. Exported for
// the selftest and used by --merge-cache (CI push-retry replay).
export function mergeAbsCache(cache, other) {
  let took = 0;
  for (const [k, v] of Object.entries(other || {})) {
    if (!v || typeof v !== 'object') continue;
    const cur = cache[k];
    if (!cur || (v.a && (!cur.a || v.a.length > cur.a.length))) { cache[k] = v; took++; }
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

async function main() {
  const args = process.argv.slice(2);
  const APPLY_ONLY = args.includes('--apply-only');
  const DRY = args.includes('--dry-run');
  const MERGE = (() => {
    const eq = args.find(a => a.startsWith('--merge-cache='));
    if (eq) return eq.slice('--merge-cache='.length);
    const i = args.indexOf('--merge-cache');
    return i >= 0 ? String(args[i + 1] || '') : '';
  })();

  const rawCache = await loadJson(CACHE_PATH, {});
  const cache = rawCache.map || rawCache;
  if (MERGE) {
    const took = mergeAbsCache(cache, (await loadJson(MERGE, {})).map || await loadJson(MERGE, {}));
    console.log(`  merge-cache: took ${took} entries from ${MERGE}`);
  }

  async function saveCache() {
    const sorted = {};
    for (const k of Object.keys(cache).sort()) sorted[k] = cache[k];
    await awrite(CACHE_PATH, JSON.stringify(sorted));
  }

  async function applyToPapers() {
    let total = 0;
    for (const f of paperFiles()) {
      const p = join(DATA_DIR, f);
      const rows = await loadJson(p, null);
      if (!rows) continue;
      let up = 0;
      for (const row of rows) {
        const rec = cache[bareDoi(row)];
        if (!rec || !rec.a) continue;
        if (betterAbstract(row.Abstract, rec.a)) { row.Abstract = rec.a.slice(0, ABS_MAX); up++; }
      }
      if (up) { await awrite(p, JSON.stringify(rows)); total += up; console.log(`  ${f}: upgraded ${up} abstracts`); }
    }
    console.log(total
      ? `✓ Applied the API cache to the served FT50 papers files (${total} abstracts).`
      : '  FT50 papers files already carry every cached abstract — nothing to apply.');
  }

  if (APPLY_ONLY) {
    if (MERGE) await saveCache();
    await applyToPapers();
    return;
  }

  // Needy list: every FT50 row with a missing/stub abstract, newest first,
  // skipping cached hits and fresh misses.
  const needy = [];
  for (const f of paperFiles()) {
    const rows = await loadJson(join(DATA_DIR, f), []);
    for (const row of rows) {
      if (!isNeedy(row)) continue;
      const doi = bareDoi(row);
      if (!doi) continue;
      const cur = cache[doi];
      if (cur && cur.a) continue;
      if (cur && cur.none && (day() - (cur.t || 0)) < MISS_TTL_DAYS &&
          !(ELS_KEY && ELS_PREFIX.test(doi))) continue; // keyed run: retry Elsevier misses now
      needy.push({ doi, y: parseInt(row.Year, 10) || 0 });
    }
  }
  needy.sort((a, b) => b.y - a.y);
  console.log(`${needy.length} FT50 papers need an abstract (missing/stub, cache-eligible).`);
  if (DRY) return;

  let s2ok = USE_S2, elsOk = true, found = 0, checked = 0, batches = 0;
  const elsStats = { found: 0, empty: 0, e404: 0, other: 0 };
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  for (let i = 0; i < needy.length; i += 50) {
    if (Date.now() - T0 > BUDGET_MS) { console.log('⏱ budget spent — stopping this slice (resume-safe).'); break; }
    const batch = needy.slice(i, i + 50).map(n => n.doi);
    let unresolved = new Set(batch);
    // Leg 1: OpenAlex
    try {
      const url = `https://api.openalex.org/works?filter=doi:${batch.join('|')}` +
        `&per-page=50&select=doi,abstract_inverted_index&mailto=${MAILTO}`;
      const r = await fetch(url, { headers: { 'User-Agent': `lit-ft50-abstracts/1.0 (mailto:${MAILTO})` } });
      if (r.status === 429 || r.status === 403) { console.log(`OpenAlex throttled (HTTP ${r.status}) — ending the run cleanly.`); break; }
      if (r.ok) {
        for (const w of (await r.json()).results || []) {
          const doi = String(w.doi || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
          const text = cleanText(invertedToText(w.abstract_inverted_index));
          if (doi && text.length >= 60) { cache[doi] = { a: text.slice(0, ABS_MAX) }; unresolved.delete(doi); found++; }
        }
      }
    } catch (e) { console.warn(`  OpenAlex batch failed: ${e.message}`); }
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
            const a = rec && typeof rec.abstract === 'string' ? cleanText(rec.abstract) : '';
            if (a.length >= 60) { cache[ids[idx]] = { a: a.slice(0, ABS_MAX) }; unresolved.delete(ids[idx]); found++; }
          });
        }
      } catch (e) { s2ok = false; console.warn(`  Semantic Scholar leg failed (${e.message}) — dropping it for this run.`); }
    }
    // Leg 3: Elsevier Abstract Retrieval (keyed; Elsevier DOIs only). One GET
    // per DOI — the leg drops out for the run on 401/403/429 so a spent weekly
    // quota can never stall the batched legs.
    if (elsOk && ELS_KEY && unresolved.size) {
      for (const doi of [...unresolved]) {
        if (!ELS_PREFIX.test(doi)) continue;
        if (Date.now() - T0 > BUDGET_MS) break;
        try {
          // view=META_ABS is what includes dc:description — the default view
          // returns metadata WITHOUT the abstract text.
          const r = await fetch(
            `https://api.elsevier.com/content/abstract/doi/${encodeURIComponent(doi)}?view=META_ABS&httpAccept=application/json`,
            { headers: { 'X-ELS-APIKey': ELS_KEY, Accept: 'application/json',
              ...(ELS_INSTTOKEN ? { 'X-ELS-Insttoken': ELS_INSTTOKEN } : {}) } });
          if (r.status === 401 || r.status === 403 || r.status === 429) {
            elsOk = false; console.log(`  Elsevier leg dropped for this run (HTTP ${r.status} — key/quota/entitlement).`); break;
          }
          if (r.ok) {
            const text = elsevierAbstract(await r.json());
            if (text.length >= 60) { cache[doi] = { a: text.slice(0, ABS_MAX) }; unresolved.delete(doi); found++; elsStats.found++; }
            else elsStats.empty++;
          } else if (r.status === 404) elsStats.e404++;
          else elsStats.other++;
        } catch (e) { elsOk = false; console.warn(`  Elsevier leg failed (${e.message}) — dropping it for this run.`); break; }
        await sleep(ELS_PACE_MS);
      }
    }
    // All legs concluded for this batch: stamp the rest as misses (TTL-retried).
    for (const doi of unresolved) cache[doi] = { none: 1, t: day() };
    checked += batch.length;
    if (++batches % 5 === 0) { await saveCache(); console.log(`  …${checked} DOIs checked, ${found} abstracts found`); }
    await sleep(PACE_MS);
  }

  await saveCache();
  const withA = Object.values(cache).filter(v => v && v.a).length;
  console.log(`\n✓ Wrote ${CACHE_PATH}`);
  console.log(`  This run: ${checked} DOIs checked, ${found} abstracts found.`);
  if (ELS_KEY && (elsStats.found + elsStats.empty + elsStats.e404 + elsStats.other)) {
    console.log(`  Elsevier leg: ${elsStats.found} found, ${elsStats.empty} 200-but-no-abstract, ` +
      `${elsStats.e404} not-found, ${elsStats.other} other.`);
    if (elsStats.empty > 20 && elsStats.found < elsStats.empty / 10) {
      console.log('  ↳ mostly empty responses: the key is likely missing off-campus ABSTRACT entitlement — ' +
        'request an institutional token (X-ELS-Insttoken) via dev.elsevier.com support and set ELSEVIER_INST_TOKEN.');
    }
  }
  console.log(`  Cache now maps ${Object.keys(cache).length} DOIs (${withA} with abstracts).`);
  await applyToPapers();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(e => { console.error(e); process.exit(1); });
}
