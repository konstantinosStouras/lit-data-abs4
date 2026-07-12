/*
 * build-data.mjs — the lit-data-abs4 data pipeline (a lit satellite data shard).
 * ===========================================================================
 * This repo is a SATELLITE DATA SHARD for stouras.com/fun/lit/ ("The Lit"):
 * ABS 4/4* journals beyond the FT50/UTD24 lists.
 * GitHub Pages caps a published site at 1 GB, so the lit catalog is sharded
 * across data repos; each shard carries its own journal list, pipeline and
 * daily workflow, publishes /data/ on its own Pages site (served under
 * stouras.com/lit-data-abs4/data/, same origin as the lit page), and the lit
 * page merges every shard's sources.json manifest at runtime.
 *
 * This file is an adapted copy of the main repo's proven pipeline at
 * konstantinosStouras.github.io/fun/lit/_scraper-ft50/build-data.mjs (itself
 * vendored from the retired fun/ft50 app). It pulls every article of every
 * journal in _scraper/journals.json from the Crossref REST API and writes
 * static JSON into /data/. No server, no database.
 *
 * The journal list is curated by hand: one entry per journal (key, name,
 * ISSNs, publisher, `abs` grade per the Chartered ABS Academic Journal Guide
 * 2024 / journalranking.org, capability flags). To add a journal: append an
 * entry (its `abs` grade verified against journalranking.org!) and push —
 * the workflow harvests it. To drop one: set "retired": true.
 * _scraper/verify-issns.mjs cross-checks every ISSN against Crossref's
 * journal registry on each run.
 *
 * What it writes into /data/:
 *   papers-<key>.json    one file per journal — the main dataset
 *   sources.json         manifest: per-journal names, files, counts, flags,
 *                        and each journal's `abs` grade (read by the lit page)
 *   authors.json / affiliations.json / recent.json / meta.json / _registry.json
 *
 * Resilience: a failed Crossref pull for one journal (or a pull that suddenly
 * shrinks below half its committed size) never sinks the build — the
 * previously committed papers-<key>.json is reused and the run continues.
 *
 * Offline smoke test (no network, uses _scraper/mock/):
 *   FT50_MOCK=1 node build-data.mjs
 * Partial run (other journals reuse their committed files):
 *   FT50_ONLY=<key1>,<key2> node build-data.mjs
 * (The FT50_* env names are inherited from the parent pipeline.)
 *
 * Node 20+ only (global fetch). No npm dependencies on purpose.
 * ===========================================================================
 */

import { readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseInformsEditors } from './informs-editors.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const MOCK = process.env.FT50_MOCK === '1';
// Mock runs write to a scratch dir so a smoke test can never pollute the live
// data/ (in particular its _registry.json, which drives "recently added").
const DATA_DIR = process.env.FT50_DATA_DIR
  || (MOCK ? resolve(__dirname, '_mock-out') : resolve(__dirname, '..', 'data'));
const MOCK_DIR = join(__dirname, 'mock');

const MAILTO = process.env.FT50_MAILTO || 'kstouras@gmail.com';
const ROWS = 1000;                  // Crossref max page size
const PAGE_PAUSE_MS = 120;          // politeness pause between cursor pages
const JOURNAL_PAUSE_MS = 500;       // pause between journals
const RECENT_WINDOW_DAYS = 90;      // buffer; the page shows the last 4 weeks
const SEED_PER_SOURCE = 3;          // onboarding: newest N per journal count as "just added"
const INFLUX_PER_SOURCE = 250;      // more unseen papers than this at once = onboarding, not news
const RECENT_CAP = 1500;            // recent.json is pre-fetched on every page load
const TOP_AFFILIATIONS = 3000;
const MAX_ABSTRACT = 4000;          // chars; keeps the big files bounded
const PULL_DATE = process.env.FT50_PULL_DATE || new Date().toISOString().slice(0, 10);
const ONLY = (process.env.FT50_ONLY || '').split(',').map(s => s.trim()).filter(Boolean);

// ── The FT50 journal list (data-driven; see check-ft50-list.mjs) ────────────

const JOURNALS_PATH = join(__dirname, 'journals.json');
const ALL_JOURNALS = JSON.parse(await readFile(JOURNALS_PATH, 'utf8'));
const JOURNALS = ALL_JOURNALS.filter(j => !j.retired);
const RETIRED = ALL_JOURNALS.filter(j => j.retired);
// Sharded journals live on satellite data sites (their own repo + GitHub
// Pages + pipeline, for growing past this repo's 1 GB Pages limit): this
// build neither pulls nor writes them — it only forwards their manifest
// entry (key/name/base/flags), so the page knows where to fetch their file.
const LOCAL_JOURNALS = JOURNALS.filter(j => !j.base);
const SHARDED_JOURNALS = JOURNALS.filter(j => j.base);

// One-time import of MS editor/area data collected by the old Google-Sheet
// pipeline from sources that don't exist on Crossref. Shared with fun/ms & lit.
const MS_OVERRIDES_PATH = resolve(__dirname, '..', '..', 'ms', '_scraper', 'editor-overrides.json');
const MS_OVERRIDES = existsSync(MS_OVERRIDES_PATH)
  ? JSON.parse(await readFile(MS_OVERRIDES_PATH, 'utf8'))
  : {};

// Curated volume/issue fixups (keyed by DOI, shared across journals) for
// advance-access records that Crossref froze without a volume/issue — otherwise
// they read as "Articles in Advance" forever. { "<doi>": { volume, issue, page?,
// year? } }. Filled only when Crossref itself still returns none.
const AIA_FIXUPS_PATH = MOCK ? join(MOCK_DIR, 'aia-fixups.json') : join(DATA_DIR, '_aia-fixups.json');
const AIA_FIXUPS = existsSync(AIA_FIXUPS_PATH)
  ? JSON.parse(await readFile(AIA_FIXUPS_PATH, 'utf8'))
  : {};

// Forthcoming papers a publisher lists on its "Articles in Advance" page but
// Crossref has not indexed yet. Built on a personal machine by
// _scraper/informs-aia-local.mjs (some publisher sites block cloud IPs) and
// committed here; each entry names its source ("jkey"). Merged in main().
// { "<doi>": { jkey, Title, Authors?, Affiliations?, Abstract?, 'Accepting Editor'?, Area?, Year? } }.
const AIA_SUPPLEMENT_PATH = MOCK ? join(MOCK_DIR, 'informs-aia.json') : join(DATA_DIR, '_informs-aia.json');
const AIA_SUPPLEMENT = existsSync(AIA_SUPPLEMENT_PATH)
  ? JSON.parse(await readFile(AIA_SUPPLEMENT_PATH, 'utf8'))
  : {};

// ── Generic fetch helpers ───────────────────────────────────────────────────

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchJson(url, attempt = 0) {
  try {
    const res = await fetch(url, { headers: { 'User-Agent': `ft50-scraper/1.0 (mailto:${MAILTO})` } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (e) {
    if (attempt >= 5) throw e;
    const wait = 2000 * Math.pow(2, attempt); // 2s…32s
    console.warn(`  fetch failed (${e.message}); retry ${attempt + 1} in ${wait}ms  [${url.slice(0, 96)}…]`);
    await sleep(wait);
    return fetchJson(url, attempt + 1);
  }
}

async function loadJsonIfExists(path, fallback) {
  if (!existsSync(path)) return fallback;
  try { return JSON.parse(await readFile(path, 'utf8')); } catch { return fallback; }
}

// ── Crossref record → paper row ─────────────────────────────────────────────

const SELECT = [
  'DOI', 'title', 'author', 'issued', 'published-print', 'published-online',
  'created', 'volume', 'issue', 'page', 'abstract', 'type', 'group-title',
  'subject', 'container-title', 'short-container-title', 'assertion',
  'is-referenced-by-count',
].join(',');

function stripJats(s) {
  if (!s) return '';
  return String(s)
    .replace(/<[^>]+>/g, ' ')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ')
    .trim();
}

function yearOf(item) {
  const pick = (d) => d && d['date-parts'] && d['date-parts'][0] && d['date-parts'][0][0];
  return String(
    pick(item.issued) || pick(item['published-print']) ||
    pick(item['published-online']) || pick(item.created) || ''
  ).replace(/^0+$/, '');
}

// Display name with no internal comma (the page splits Authors on commas).
function authorName(a) {
  const nm = [a.given, a.family].filter(Boolean).join(' ') || a.name || '';
  return nm.replace(/,/g, ' ').replace(/\s+/g, ' ').trim();
}

function normTitle(t) {
  return String(t || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

// ── Management Science editor/area extraction (ported from fun/ms & lit) ───
// Only applied to records of journals with editors:true (MS alone).

function stripTrailers(s) {
  return s
    .replace(/\.?\s*(funding|supplemental material|history|data|acknowledgments?|conflicts?[^:]{0,30}|epub)\s*:.*$/i, '')
    .replace(/\.?\s*https?:\/\/.*$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function plausibleEditorName(s) {
  if (!s || s.length > 70) return false;
  if (s.split(/\s+/).length > 8) return false;
  if (!/^[A-ZÀ-Þ]/.test(s)) return false;
  return !/\b(the|this|that|is|are|was|were|we|when|which|of|by|in|on|to|as|editors?)\b/i.test(s);
}

function acceptance(abstractText) {
  let m = abstractText.match(/(?:this\s+)?(?:paper|work)\s+(?:was|has\s+been)\s+accepted by\s+([^.]+(?:\.[^.]{0,5})*[^.]*)\./i);
  if (!m) m = abstractText.match(/accepted by\s+([^.]+(?:\.[^.]{0,5})*[^.]*)\./i);
  if (!m) return { editor: '', area: '' };
  let body = stripTrailers(m[1].trim());
  const comma = body.indexOf(',');
  let area = comma !== -1 ? body.slice(comma + 1).trim() : '';
  if (comma !== -1) body = body.slice(0, comma).trim();
  if (area) area = area.split(/\.\s/)[0].replace(/\.$/, '').trim();
  const si = body.match(/^(.*?)\s+for\s+the\s+(.*(?:special\s+issue|special\s+section).*)$/i);
  if (si) { body = si[1].trim(); area = si[2].trim(); }
  if (!plausibleEditorName(body)) return { editor: '', area: '' };
  return {
    editor: 'This paper was accepted by ' + body + (area ? ', ' + area : '') + '.',
    area,
  };
}

function normArea(s) {
  const a = (s || '').replace(/<[^>]+>/g, '').replace(/\.\s*$/, '').trim().toLowerCase();
  if (/special issue on (the )?digital finance/.test(a)) return 'special issue on the digital finance';
  return a;
}

function assertionEditor(item) {
  for (const a of item.assertion || []) {
    const label = ((a.label || a.name || '') + '').toLowerCase();
    if (label.includes('editor')) {
      const v = stripJats(a.value || '');
      if (!v) return '';
      return /accepted by/i.test(v) ? v.replace(/\.?$/, '.')
        : 'This paper was accepted by ' + v.replace(/\.$/, '') + '.';
    }
  }
  return '';
}

// ── mapping ────────────────────────────────────────────────────────────────

function mapWork(item, src) {
  const title = (item.title && item.title[0]) ? stripJats(item.title[0]) : '';
  if (!title) return null;

  // Keep names and ORCIDs aligned: filter nameless author entries *before*
  // pairing, or one nameless entry shifts every later ORCID onto the wrong
  // author (which would then poison the ORCID-based merging in authors.json).
  const authorPairs = (item.author || [])
    .map(a => ({ name: authorName(a), orcid: (a.ORCID || '').replace(/^https?:\/\/orcid\.org\//, '') }))
    .filter(x => x.name);
  const authorsArr = authorPairs.map(x => x.name);
  const affSet = new Set();
  (item.author || []).forEach(a => (a.affiliation || []).forEach(af => {
    if (af && af.name) affSet.add(af.name.replace(/\s+/g, ' ').trim());
  }));

  const abstract = stripJats(item.abstract || '').slice(0, MAX_ABSTRACT);

  // Editors/Areas: Management Science only (per the page's design).
  let editor = '', area = '';
  if (src.editors) {
    const acc = acceptance(abstract);
    editor = acc.editor;
    let accArea = acc.area;
    if (!editor) editor = assertionEditor(item);
    const ov = MS_OVERRIDES[(item.DOI || '').toLowerCase()];
    if (!editor && ov) editor = 'This paper was accepted by ' + ov.editor.replace(/\.+$/, '') + '.';
    const groupTitle = Array.isArray(item['group-title']) ? item['group-title'][0] : item['group-title'];
    const subject = Array.isArray(item.subject) ? item.subject[0] : '';
    area = normArea(accArea) || normArea(ov && ov.area) || normArea(groupTitle) || normArea(subject) || '';
  }

  // Senior/Associate Editor from the History line, when the Crossref abstract
  // carries it (ISR both, Marketing Science senior only). The committed
  // pubsonline cache fills the rest — see applyInformsEditors().
  let se = '', ae = '';
  if (src.seEditors) {
    const ed = parseInformsEditors(abstract);
    se = ed.se;
    if (src.aeEditors) ae = ed.ae;
  }

  let volume = item.volume || '';
  let issue = item.issue || '';
  let page = item.page || '';
  let year = yearOf(item);
  // Fill a frozen advance-access record's real issue from the curated fixups so
  // it reads as published, not perpetually "Articles in Advance". Crossref wins
  // whenever it actually carries a volume/issue.
  const fx = AIA_FIXUPS[(item.DOI || '').toLowerCase()];
  if (fx) {
    if (!volume && fx.volume != null && fx.volume !== '') volume = String(fx.volume);
    if (!issue && fx.issue != null && fx.issue !== '') issue = String(fx.issue);
    if (!page && fx.page) page = String(fx.page);
    if (fx.year) year = String(fx.year);
  }
  const status = src.aia ? forthcomingStatus(volume, issue, year, PULL_DATE) : '';
  const doi = item.DOI ? 'https://doi.org/' + item.DOI : '';

  const row = {
    Title: title,
    Authors: authorsArr.join(', '),
    Affiliations: [...affSet].join('; '),
    DOI: doi,
    Volume: String(volume),
    Issue: String(issue),
    Page: page,
    Year: year,
    Status: status,
    Abstract: abstract,
    'Accepting Editor': editor,
    Area: area,
    Journal: src.name,
    JKey: src.key,
    // internal, dropped before writing:
    _doi: (item.DOI || '').toLowerCase(),
    _orcids: authorPairs.map(x => x.orcid),
    _rank: pubRank(year, volume, issue, page, status),
  };
  if (src.seEditors) row['Senior Editor'] = se;
  if (src.aeEditors) row['Associate Editor'] = ae;
  // Crossref citation count (is-referenced-by-count) — a different, lower metric
  // than Google Scholar's; the lit page labels it "Cited by N · Crossref". Omit
  // zero/missing so it never bloats the papers files or shows a "Cited by 0".
  const citedBy = item['is-referenced-by-count'];
  if (typeof citedBy === 'number' && citedBy > 0) row.CitedBy = citedBy;
  return row;
}

// Overlay Senior/Associate Editor names from the committed cache built by
// fun/lit/_scraper/informs-editors-local.mjs on a personal machine (Crossref
// rarely carries the History line; pubsonline.informs.org blocks cloud IPs).
// Cache shape: { "<doi>": { se: "Name; Name", ae: "Name" } }. The lit cache is
// shared (same DOIs); an ft50-local cache, if ever committed, wins per-DOI.
async function applyInformsEditors(bySource) {
  const litPath = resolve(__dirname, '..', '..', 'lit', 'data', '_informs-editors.json');
  const ownPath = MOCK ? join(MOCK_DIR, 'informs-editors.json') : join(DATA_DIR, '_informs-editors.json');
  const litCache = MOCK ? {} : await loadJsonIfExists(litPath, {});
  const ownCache = await loadJsonIfExists(ownPath, {});
  const map = { ...(litCache.map || litCache), ...(ownCache.map || ownCache) };
  let filled = 0;
  for (const src of JOURNALS) {
    if (!src.seEditors) continue;
    for (const p of bySource[src.key] || []) {
      const rec = map[p._doi];
      if (!rec) continue;
      if (!p['Senior Editor'] && rec.se) { p['Senior Editor'] = rec.se; filled++; }
      if (src.aeEditors && !p['Associate Editor'] && rec.ae) { p['Associate Editor'] = rec.ae; filled++; }
    }
  }
  if (filled) console.log(`  informs editors: filled ${filled} SE/AE fields from the cache`);
}

function pubRank(year, volume, issue, page, status) {
  const aia = status ? 1 : 0; // any non-published status ranks above published
  const y = parseInt(year, 10) || 0;
  const v = parseInt(volume, 10) || 0;
  const iss = parseInt(issue, 10) || 0;
  const p = parseInt(String(page || '').split(/[-–]/)[0], 10) || 0;
  return aia * 1e13 + y * 1e9 + v * 1e6 + Math.min(iss, 999) * 1e3 + Math.min(p, 999);
}

// A no-volume/no-issue article counts as "Articles in Advance" only if it is
// recent; an older one is a published paper whose Crossref record was frozen at
// the advance stage (fill its issue via _aia-fixups.json), not a forthcoming one.
function forthcomingStatus(volume, issue, year, pullDate) {
  if (volume || issue) return '';
  const py = parseInt(String(pullDate).slice(0, 4), 10) || 0;
  const y = parseInt(year, 10) || 0;
  return (y && y >= py - 3) ? 'Articles in Advance' : '';
}

// registry key: DOI when there is one, else a title|year key
function regKey(row) {
  return row._doi || ('t:' + normTitle(row.Title) + '|' + row.Year);
}

// Rebuild the internal fields of a row read back from a committed
// papers-<key>.json (publicRow stripped them before writing). ORCIDs are
// preserved across the reuse cycle via the pipe-joined `Orcids` field that
// publicRow writes (empty slots kept, so index alignment with the Authors list
// survives) — otherwise a journal whose pull is reused would lose ORCID-based
// author merging in authors.json.
function rehydrateRow(row) {
  row._doi = String(row.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
  row._orcids = typeof row.Orcids === 'string' ? row.Orcids.split('|') : [];
  delete row.Orcids;
  row._rank = pubRank(row.Year, row.Volume, row.Issue, row.Page, row.Status);
  return row;
}

// ── journal pulls ───────────────────────────────────────────────────────────

async function fetchJournalWorks(src) {
  if (MOCK) {
    const raw = await loadJsonIfExists(join(MOCK_DIR, `crossref-${src.key}.json`), null);
    if (!raw) return null; // no fixture -> journal absent from the mock build
    const items = raw.message ? raw.message.items : raw;
    console.log(`  [mock] ${src.key}: ${items.length} items`);
    return items;
  }
  const all = [];
  for (let i = 0; i < src.issns.length; i++) {
    const issn = src.issns[i];
    const before = all.length;
    try {
      const base = `https://api.crossref.org/journals/${issn}/works`;
      let cursor = '*';
      let page = 0;
      for (;;) {
        const url = `${base}?rows=${ROWS}&cursor=${encodeURIComponent(cursor)}` +
          `&select=${encodeURIComponent(SELECT)}&mailto=${encodeURIComponent(MAILTO)}`;
        const body = await fetchJson(url);
        const items = body.message.items || [];
        all.push(...items);
        page++;
        if (page % 10 === 0 || !items.length) {
          console.log(`  ${src.key}/${issn} page ${page}: running total ${all.length}/${body.message['total-results']}`);
        }
        cursor = body.message['next-cursor'];
        if (!items.length || !cursor) break;
        await sleep(PAGE_PAUSE_MS);
      }
      console.log(`  ${src.key}/${issn}: +${all.length - before} records`);
    } catch (e) {
      // The primary ISSN must succeed; secondary/predecessor ISSNs are
      // best-effort (a 404 there must not sink the journal).
      if (i === 0) throw e;
      console.warn(`  ${src.key}/${issn}: skipped (${e.message})`);
    }
  }
  return all; // mapJournal dedupes by DOI across ISSNs
}

function mapJournal(rawWorks, src) {
  const seen = new Set();
  const papers = [];
  for (const item of rawWorks) {
    if (item.type && item.type !== 'journal-article') continue;
    const row = mapWork(item, src);
    if (!row) continue;
    if (row._doi && seen.has(row._doi)) continue;
    if (row._doi) seen.add(row._doi);
    papers.push(row);
  }
  return papers;
}

// Load the previously committed papers-<key>.json (rehydrated), or [].
async function loadCommitted(src) {
  const rows = await loadJsonIfExists(join(DATA_DIR, `papers-${src.key}.json`), []);
  return (Array.isArray(rows) ? rows : []).map(rehydrateRow);
}

// Fetch one journal with full failure tolerance: on any error — or on a pull
// that shrinks below half the committed size (a truncated walk, a mis-served
// journal record) — keep the previously committed dataset for this journal.
async function pullJournal(src) {
  const committed = await loadCommitted(src);
  if (ONLY.length && !ONLY.includes(src.key)) {
    console.log(`  ${src.key}: not in FT50_ONLY — reusing the committed file (${committed.length} papers)`);
    return committed;
  }
  let fresh;
  try {
    const raw = await fetchJournalWorks(src);
    if (raw === null) return committed; // mock without fixture
    fresh = mapJournal(raw, src);
  } catch (e) {
    console.warn(`  ${src.key}: pull FAILED (${e.message}) — reusing the committed file (${committed.length} papers)`);
    return committed;
  }
  if (committed.length && fresh.length < committed.length * 0.5 && process.env.FT50_ALLOW_SHRINK !== '1') {
    console.warn(`  ${src.key}: fresh pull holds only ${fresh.length} papers vs ${committed.length} committed — ` +
      'suspicious shrink, reusing the committed file (set FT50_ALLOW_SHRINK=1 to accept it)');
    return committed;
  }
  return fresh;
}

// ── Pre-print (arXiv/SSRN) open-access links, for every source ──────────────
// Any paper with a free author pre-print on arXiv or SSRN gets a `Preprint`
// URL (+ `PreprintSrc`), surfaced on the card as an open-access link. Resolved
// from OpenAlex by DOI — batched exactly like enrichEc — and cached in
// the dataset dir’s _preprints.json (doi -> {u,s} | {none:1}) so the daily build only
// queries DOIs it has not resolved before. Non-fatal end to end: a lookup that
// fails just leaves that paper without a link.

// Canonical arXiv landing URL: strip any pinned version suffix (v2) and any
// .pdf tail so the link always resolves to the LATEST version of the paper.
export function canonArxiv(u) {
  const m = String(u || '').match(/^https?:\/\/(?:www\.|export\.)?arxiv\.org\/(?:abs|pdf)\/([^?#]+)/i);
  if (!m) return u;
  const id = m[1].replace(/\.pdf$/i, '').replace(/v\d+$/i, '');
  return `https://arxiv.org/abs/${id}`;
}

// Cached finds may predate the latest-version canonicalisation — normalise on
// every apply so rows always carry the canonical (latest-version) URL.
export function canonPreprint(u) { return canonArxiv(u); }

export function pickPreprint(cands) {
  // http(s) only, and matched on the real hostname (not a substring) so a
  // spoofed host like arxiv.org.evil.com cannot slip into the href. Preference
  // order: arXiv > SSRN (the hosts the paper asked for; arXiv links are
  // stabler) > bioRxiv/medRxiv > NBER > OSF (the broader repositories).
  const host = (u) => { try { return new URL(u).hostname.toLowerCase(); } catch { return ''; } };
  const isHost = (h, d) => h === d || h.endsWith('.' + d);
  const list = cands.filter(u => u && /^https?:\/\//i.test(u));
  const find = (d) => list.find(u => isHost(host(u), d));
  const arx = find('arxiv.org');
  if (arx) return { u: canonArxiv(arx), s: 'arxiv' };
  const ssrn = find('ssrn.com');
  if (ssrn) return { u: ssrn, s: 'ssrn' };
  const bio = find('biorxiv.org');
  if (bio) return { u: bio, s: 'biorxiv' };
  const med = find('medrxiv.org');
  if (med) return { u: med, s: 'medrxiv' };
  // NBER: accept both the landing (/papers/wN) and the direct-PDF
  // (/system/files/working_papers/wN/wN.pdf) forms — OpenAlex often supplies
  // only the latter — and canonicalise to the stable landing URL.
  const nber = list.find(u => isHost(host(u), 'nber.org') &&
    /\/(?:papers|system\/files\/working_papers)\/w\d+/i.test(u));
  if (nber) return { u: `https://www.nber.org/papers/${nber.match(/\/(w\d+)/i)[1].toLowerCase()}`, s: 'nber' };
  // OSF: only the preprint server's own pages (osf.io/preprints/...) — a bare
  // osf.io guid is just as often a project or registration, not a pre-print
  // (those still arrive via the 10.31219 OSF-preprint DOI in preprintFromDoi).
  const osf = list.find(u => isHost(host(u), 'osf.io') && /\/preprints\//i.test(u));
  if (osf) return { u: osf, s: 'osf' };
  return null;
}

// An SSRN/arXiv preprint record has its own DOI; turn it into a landing URL.
export function preprintFromDoi(doi) {
  const d = String(doi || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
  let m = d.match(/^10\.2139\/ssrn\.(\d+)$/);        // SSRN
  if (m) return { u: `https://papers.ssrn.com/sol3/papers.cfm?abstract_id=${m[1]}`, s: 'ssrn' };
  m = d.match(/^10\.48550\/arxiv\.(.+)$/);           // arXiv (DataCite DOI)
  if (m) return { u: `https://arxiv.org/abs/${m[1].replace(/v\d+$/i, '')}`, s: 'arxiv' };
  // bioRxiv/medRxiv preprint DOIs only: date-coded (2023.01.02.522345) or
  // legacy numeric (121212). The same 10.1101 prefix also covers CSHL Press
  // JOURNALS (gr.*, gad.*, cshperspect.*, pdb.*) — paywalled articles that
  // must never be surfaced as an open-access pre-print. The DOI alone can't
  // say WHICH rxiv hosts it, so the source is the neutral 'cshl'.
  m = d.match(/^10\.1101\/((?:\d{4}\.\d{2}\.\d{2}\.)?\d+)$/);
  if (m) return { u: `https://doi.org/10.1101/${m[1]}`, s: 'cshl' };
  m = d.match(/^10\.3386\/(w\d+)$/i);                // NBER working paper
  if (m) return { u: `https://www.nber.org/papers/${m[1].toLowerCase()}`, s: 'nber' };
  m = d.match(/^10\.31219\/osf\.io\/(\w+)$/);        // OSF preprint
  if (m) return { u: `https://osf.io/${m[1]}`, s: 'osf' };
  return null;
}

// Normalized last-name tokens from "First Last" name strings.
function lastNames(names) {
  const out = new Set();
  for (const n of names) {
    const toks = String(n || '').trim().split(/\s+/);
    const norm = (toks[toks.length - 1] || '').toLowerCase()
      .normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/[^a-z]/g, '');
    if (norm.length >= 2) out.add(norm);
  }
  return out;
}

// Fully-collapsing title norm for preprint matching, same shape as the
// reference pipeline's (fun/lit/_scraper) ec-pages normTitle: lowercase, NFD
// accent-fold, then strip ALL non-alphanumerics so 'Trade-offs' == 'Tradeoffs'.
// Deliberately NOT this file's registry normTitle, which keeps word gaps.
const matchNorm = (s) => String(s || '').toLowerCase()
  .normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '');

// Among OpenAlex title-search results, find the SAME paper's arXiv/SSRN
// preprint record. Conservative on purpose (a wrong link is worse than none):
// requires an exact-or-prefix title match (titlesMatch), two shared author
// surnames (one for single-author records), a plausible year, and only accepts an arXiv/SSRN-hosted location or preprint
// DOI. Pure → unit-tested.
export function matchPreprintWork(paper, results) {
  const nt = matchNorm(paper.title || '');
  if (!nt) return null;
  const py = parseInt(paper.year, 10);
  const mine = lastNames(String(paper.authors || '').split(','));
  for (const w of results || []) {
    const tmw = titlesMatch(nt, matchNorm(w.title || ''));
    if (!tmw) continue;
    const wn = lastNames((w.authorships || []).map(a => (a.author && a.author.display_name) || ''));
    // Double-check the authors, not just one of them: require two shared
    // surnames whenever both records list two or more authors (a single
    // shared surname suffices only for single-author records) — an exact
    // title alone ("Introduction", "Repeated Games") is no proof of identity.
    if (!mine.size || !wn.size) continue;
    const need = Math.min(2, mine.size, wn.size);
    let shared = 0;
    for (const x of wn) if (mine.has(x)) shared++;
    if (shared < need) continue;
    const wy = parseInt(w.publication_year, 10);
    // A preprint precedes publication; a PREFIX match must additionally be
    // near-contemporaneous, or it is likely a same-team title-stem sibling.
    if (py && wy && (wy > py + 1 || wy < py - (tmw === 'exact' ? 12 : 6))) continue;
    const urls = [];
    for (const loc of w.locations || []) if (loc) urls.push(loc.landing_page_url, loc.pdf_url);
    if (w.best_oa_location) urls.push(w.best_oa_location.landing_page_url, w.best_oa_location.pdf_url);
    if (w.primary_location) urls.push(w.primary_location.landing_page_url, w.primary_location.pdf_url);
    const pick = pickPreprint(urls) || preprintFromDoi(w.doi);
    if (pick) return pick;
  }
  return null;
}

// Titles match when the collapsed forms are equal, or one is a PREFIX of the
// other — a working paper often gains or loses a subtitle on publication
// ("Dueling Contests" vs "Dueling Contests and Platform's Coordinating
// Role"). Never below 14 collapsed chars, and only ever used together with
// the two-surname author check, so a title stem alone can't link a paper.
function titlesMatch(a, b) {
  if (!a || !b) return '';
  if (a === b) return 'exact';
  const s = a.length <= b.length ? a : b, l = a.length <= b.length ? b : a;
  if (s.length < 14 || !l.startsWith(s)) return '';
  // The longer title's extra words must not mark a SEPARATE follow-up
  // publication — a comment/reply/corrigendum shares both the stem and the
  // authors, and would link the WRONG paper's pre-print.
  if (/comment|repl(y|ies)|corrigend|errat|rejoinder|retract/i.test(l.slice(s.length))) return '';
  return 'prefix';
}

// Second search engine, SSRN via Crossref: OpenAlex's SSRN coverage is
// patchy — many working papers that live on SSRN have no OpenAlex record at
// all — but SSRN mints its DOIs THROUGH Crossref, so Crossref has every one
// (prefix 10.2139). Same conservative matching as matchPreprintWork; the
// pure matcher is split out for unit tests.
export function matchCrossrefPreprint(paper, items) {
  const nt = matchNorm(paper.title || '');
  if (!nt) return null;
  const py = parseInt(paper.year, 10);
  const mine = lastNames(String(paper.authors || '').split(','));
  for (const it of items || []) {
    const wt = matchNorm(String((Array.isArray(it.title) ? it.title[0] : it.title) || ''));
    const tmc = titlesMatch(nt, wt);
    if (!tmc) continue;
    const wn = lastNames((it.author || []).map(a => (a && a.family) || ''));
    if (!mine.size || !wn.size) continue;
    const need = Math.min(2, mine.size, wn.size);
    let shared = 0;
    for (const x of wn) if (mine.has(x)) shared++;
    if (shared < need) continue;
    const dp = it.issued && it.issued['date-parts'];
    const wy = parseInt(dp && dp[0] && dp[0][0], 10);
    if (py && wy && (wy > py + 1 || wy < py - (tmc === 'exact' ? 12 : 6))) continue;
    const pick = preprintFromDoi(it.DOI);
    if (pick) return pick;
  }
  return null;
}

async function searchSsrnViaCrossref(p) {
  const q = String(p.Title || '').replace(/[^\w\s'-]/g, ' ').replace(/\s+/g, ' ').trim();
  if (!q) return null;
  const url = 'https://api.crossref.org/works?query.bibliographic=' + encodeURIComponent(q) +
    '&filter=prefix:10.2139&rows=8&select=DOI,title,author,issued' +
    `&mailto=${encodeURIComponent(MAILTO)}`;
  const r = await oaGet(url);
  // A transient failure (429/timeout/outage) must NOT read as 'searched, no
  // match' — the caller leaves the paper un-stamped so a later run retries.
  if (!r.ok) return { err: 1 };
  return matchCrossrefPreprint({ title: p.Title, year: p.Year, authors: p.Authors },
    (r.json && r.json.message && r.json.message.items) || []);
}

async function resolvePreprints(allPapers, cache) {
  // 1. Seed from links we already resolved (EC's PDF is arXiv/SSRN/OA), so we
  //    never spend an OpenAlex call on a paper we can already answer.
  for (const p of allPapers) {
    if (!p._doi || cache[p._doi]) continue;
    const pick = pickPreprint([p.PDF]);
    if (pick) cache[p._doi] = pick;
  }
  if (MOCK) return; // offline: no OpenAlex; the seed above still applies.

  // 2. OpenAlex, batched 50 DOIs per request (same shape as enrichEc).
  const seen = new Set(), need = [];
  for (const p of allPapers) {
    if (!p._doi || cache[p._doi] || seen.has(p._doi)) continue;
    seen.add(p._doi); need.push(p._doi);
  }
  console.log(`  preprints: resolving ${need.length} DOIs via OpenAlex…`);
  for (let i = 0; i < need.length; i += 50) {
    const batch = need.slice(i, i + 50);
    const url = 'https://api.openalex.org/works?filter=doi:' + batch.join('|') +
      '&per-page=50&select=doi,open_access,best_oa_location,locations' +
      `&mailto=${encodeURIComponent(MAILTO)}`;
    try {
      const j = await fetchJson(url);
      const byDoi = new Map();
      for (const w of j.results || []) {
        byDoi.set(String(w.doi || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase(), w);
      }
      for (const doi of batch) {
        const w = byDoi.get(doi);
        if (!w) { cache[doi] = { none: 1 }; continue; }
        const cands = (w.locations || []).flatMap(l => [l && l.landing_page_url, l && l.pdf_url]);
        cands.push(w.best_oa_location && w.best_oa_location.landing_page_url,
                   w.best_oa_location && w.best_oa_location.pdf_url,
                   w.open_access && w.open_access.oa_url);
        cache[doi] = pickPreprint(cands) || { none: 1 };
      }
    } catch (e) {
      console.warn('  openalex preprints batch failed (non-fatal):', e.message);
    }
    await sleep(400);
  }

  // 3. Title+author search for papers the by-DOI scan couldn't link — their
  //    arXiv/SSRN preprint exists as a SEPARATE OpenAlex record (own
  //    10.2139/ssrn.* DOI). This is what surfaces most SSRN preprints. It is
  //    strictly TIME-BOUNDED and gentle (see searchPreprintsByTitle) so the
  //    daily build can never hang if OpenAlex throttles the per-paper query;
  //    the full backfill runs online via preprints-ci.mjs (own workflow).
  await searchPreprintsByTitle(allPapers, cache, {
    cap: parseInt(process.env.FT50_PREPRINT_SEARCH_CAP || '2500', 10),
    budgetMs: parseInt(process.env.FT50_PREPRINT_SEARCH_MS || '360000', 10), // 6-minute hard ceiling
    log: true,
  });
}

// A single, gentle OpenAlex GET — one attempt with a hard timeout, NO retry
// stacking. (fetchJson retries 5× with up to 62s of backoff, which is fine for
// a handful of batched calls but lets a throttled per-paper title search drag
// on for hours.) Returns {ok, status, retryAfter, json}.
async function oaGet(url) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 20000);
  try {
    const res = await fetch(url, { headers: { 'User-Agent': `lit-scraper/1.0 (mailto:${MAILTO})` }, signal: ctrl.signal });
    if (!res.ok) {
      const ra = parseInt(res.headers.get('retry-after') || '', 10);
      return { ok: false, status: res.status, retryAfter: isNaN(ra) ? 0 : ra };
    }
    return { ok: true, status: 200, json: await res.json() };
  } catch (e) {
    return { ok: false, status: 0, retryAfter: 0 };
  } finally {
    clearTimeout(timer);
  }
}

// Search-pass version. Bump whenever the matcher or the host coverage
// expands: cache entries searched under an older version become eligible
// again, so earlier misses are retried with the wider net (never-searched
// papers still go first — see the eligibility sort). v2: bioRxiv/medRxiv/
// NBER/OSF hosts, two-surname author check, year floor 1991 (arXiv's first
// year, instead of 2005).
// v3: SSRN-via-Crossref second engine, prefix-tolerant title match (working
// papers often gain/lose a subtitle on publication), OpenAlex per-page 25.
export const TS_VER = 3;

// Find each unlinked paper's arXiv/SSRN pre-print via an OpenAlex title.search,
// matched conservatively by matchPreprintWork. Cache entries become {u,s}
// (found) or {none:1,ts:TS_VER} (searched, nothing — re-eligible whenever
// TS_VER is bumped); an errored lookup is left
// without `ts` so a later run retries it. Bounded by `cap` and, when given, a
// wall-clock `budgetMs`. Throttling (429/403) is handled two ways:
//   - default (the daily build): back off briefly, and STOP for this run after
//     a few consecutive throttles — CI must never sit out a rate-limit.
//   - opts.patient (preprints-local.mjs): wait it out with escalating backoff
//     (retry-after honoured, 5s→60s) and RETRY the same paper, because a
//     personal machine can afford to ride through OpenAlex's rate-limiting.
// Returns the number newly linked.
export async function searchPreprintsByTitle(papers, cache, opts = {}) {
  const cap = opts.cap || 6000;
  const sleepMs = opts.sleepMs || 130;
  const maxThrottle = opts.patient ? 25 : (opts.maxThrottle || 6);
  const deadline = opts.budgetMs ? Date.now() + opts.budgetMs : Infinity;
  const eligible = papers
    .filter(p => p._doi && cache[p._doi] && cache[p._doi].none &&
                 (cache[p._doi].ts || 0) < TS_VER && parseInt(p.Year, 10) >= 1991)
    .sort((a, b) =>
      ((cache[a._doi].ts ? 1 : 0) - (cache[b._doi].ts ? 1 : 0)) ||
      ((parseInt(b.Year, 10) || 0) - (parseInt(a.Year, 10) || 0)));
  const todo = eligible.slice(0, cap);
  if (opts.log) console.log(`  preprints: title-searching up to ${todo.length} of ${eligible.length} unlinked papers…`);
  let found = 0, searched = 0, throttled = 0, crFails = 0;
  for (let i = 0; i < todo.length; i++) {
    const p = todo[i];
    if (Date.now() > deadline) { if (opts.log) console.log('  preprints: title-search time budget reached — resuming next run.'); break; }
    const q = String(p.Title || '').replace(/[^\w\s'-]/g, ' ').replace(/\s+/g, ' ').trim();
    if (!q) { cache[p._doi] = { none: 1, ts: TS_VER }; continue; }
    const url = 'https://api.openalex.org/works?filter=title.search:' + encodeURIComponent(q) +
      '&per-page=25&select=doi,title,publication_year,authorships,best_oa_location,primary_location,locations' +
      `&mailto=${encodeURIComponent(MAILTO)}`;
    const r = await oaGet(url);
    if (r.ok) {
      throttled = 0; searched++;
      let pick = matchPreprintWork({ title: p.Title, year: p.Year, authors: p.Authors }, (r.json && r.json.results) || []);
      let crErr = false;
      if (!pick) {
        const cr = await searchSsrnViaCrossref(p);        // second engine: SSRN lives in Crossref
        if (cr && cr.err) crErr = true; else pick = cr;
      }
      if (crErr) {
        // Crossref leg failed: leave the entry un-stamped so a later run
        // re-runs BOTH engines for this paper (same contract as an errored
        // OpenAlex lookup); a persistently failing Crossref stops the run.
        if (++crFails >= 6) { if (opts.log) console.log('  preprints: Crossref failing — stopping title-search for this run.'); break; }
      } else {
        crFails = 0;
        cache[p._doi] = pick || { none: 1, ts: TS_VER };
        if (pick) found++;
      }
      if (opts.log && searched % 500 === 0) console.log(`  preprints: …${searched} searched, ${found} linked so far`);
      // Periodic save so a long local run can be interrupted without losing work.
      if (opts.checkpoint && searched % 200 === 0) await opts.checkpoint(cache);
      await sleep(sleepMs);
    } else if (r.status === 429 || r.status === 403) {
      throttled++;                                  // leave un-ts so it retries later
      if (throttled >= maxThrottle) { if (opts.log) console.log('  preprints: OpenAlex throttling — stopping title-search for this run.'); break; }
      if (opts.patient) {
        // A Retry-After of minutes is per-second/burst throttling — wait it
        // out and retry the SAME paper. A Retry-After of HOURS means the
        // DAILY quota for this mailto/IP is spent: sleeping on it would just
        // hang the terminal, so save progress and exit with a clear message.
        const quotaMs = opts.maxWaitMs || 15 * 60 * 1000;
        if (r.retryAfter * 1000 > quotaMs) {
          const h = Math.floor(r.retryAfter / 3600), m = Math.round((r.retryAfter % 3600) / 60);
          const at = new Date(Date.now() + r.retryAfter * 1000).toISOString().slice(11, 16);
          if (opts.log) console.log(
            `  preprints: OpenAlex says the daily request quota is spent — it resets in ~${h}h ${m}m (${at} UTC).\n` +
            `  Progress is saved; simply re-run this script after that time (or with a different FT50_MAILTO).`);
          break;
        }
        const wait = Math.max(r.retryAfter * 1000, Math.min(5000 * Math.pow(2, throttled - 1), 60000));
        if (opts.log) console.log(`  preprints: rate-limited — waiting ${Math.round(wait / 1000)}s…`);
        await sleep(wait);
        i--;
      } else {
        await sleep(Math.min(Math.max(r.retryAfter, 2) * 1000, 10000));
      }
    } else {
      await sleep(500);                             // transient error: brief pause, keep going
    }
  }
  if (opts.log) console.log(`  preprints: title search linked ${found} more (searched ${searched}).`);
  return found;
}

function applyPreprints(allPapers, cache) {
  let n = 0;
  for (const p of allPapers) {
    const x = p._doi && cache[p._doi];
    if (x && x.u) { p.Preprint = canonPreprint(x.u); p.PreprintSrc = x.s; n++; }
  }
  console.log(`  preprints: ${n}/${allPapers.length} papers link to an arXiv/SSRN pre-print`);
}

// ── Registry (key -> first-seen date) ──────────────────────────────────────

async function loadRegistry() {
  const path = join(DATA_DIR, '_registry.json');
  if (!existsSync(path)) return { map: {}, firstRun: true };
  try {
    return { map: JSON.parse(await readFile(path, 'utf8')), firstRun: false };
  } catch {
    return { map: {}, firstRun: true };
  }
}

// Per-journal onboarding guard: a journal that suddenly contributes hundreds
// of unseen papers is being onboarded (first run, a new FT50 journal, an ISSN
// fix), not publishing hundreds of new articles — stamp only its newest few
// so "recently added" reflects news, never an influx. Guarding per journal
// (rather than per run, as fun/lit does) means adding one journal to the list
// can never suppress the genuinely new papers of the other 49 that day.
function updateRegistry(bySource, reg) {
  for (const src of JOURNALS) {
    const rows = bySource[src.key] || []; // already sorted newest-first
    const newKeys = [];
    for (const p of rows) {
      const k = regKey(p);
      if (!(k in reg.map)) newKeys.push(k);
    }
    if (!newKeys.length) continue;
    const baseline = reg.firstRun || newKeys.length > INFLUX_PER_SOURCE;
    if (baseline && !reg.firstRun) {
      console.log(`  registry: ${src.key} has ${newKeys.length} unseen papers at once — onboarding, not news`);
    }
    const seedSet = new Set(baseline ? newKeys.slice(0, SEED_PER_SOURCE) : []);
    for (const k of newKeys) {
      reg.map[k] = baseline ? (seedSet.has(k) ? PULL_DATE : '') : PULL_DATE;
    }
  }
  return reg.map;
}

// ── Aggregates (ported from fun/lit, source-aware) ──────────────────────────

function stripAccents(s) {
  return s.normalize('NFD').replace(/[̀-ͯ]/g, '');
}
function cmp(a, b) { return a < b ? -1 : a > b ? 1 : 0; }

function buildAuthors(papers) {
  const parent = new Map();
  const find = (k) => {
    let r = k;
    while (parent.get(r) !== r) r = parent.get(r);
    let c = k;
    while (parent.get(c) !== c) { const n = parent.get(c); parent.set(c, r); c = n; }
    return r;
  };
  const add = (k) => { if (!parent.has(k)) parent.set(k, k); return find(k); };
  const union = (a, b) => { const ra = add(a), rb = add(b); if (ra !== rb) parent.set(rb, ra); };

  const authorNames = (p) => p.Authors ? p.Authors.split(',').map(s => s.trim()).filter(Boolean) : [];
  const normName = (name) => stripAccents(name).toLowerCase();
  const nameSet = new Set();
  for (const p of papers) authorNames(p).forEach(n => nameSet.add(normName(n)));

  // ORCID merging with the same one-paper-misattribution guard as fun/ms.
  const orcidNames = new Map();
  for (const p of papers) {
    authorNames(p).forEach((name, i) => {
      add('n:' + normName(name));
      const orcid = (p._orcids && p._orcids[i]) || '';
      if (!orcid) return;
      let m = orcidNames.get(orcid);
      if (!m) { m = new Map(); orcidNames.set(orcid, m); }
      const nk = normName(name);
      m.set(nk, (m.get(nk) || 0) + 1);
    });
  }
  for (const [orcid, names] of orcidNames) {
    for (const [nk, count] of names) {
      if (names.size === 1 || count >= 2) union('n:' + nk, 'o:' + orcid);
    }
  }
  // "Hau L. Lee" == "Hau Lee" when the middle-initial-free form exists too.
  for (const n of nameSet) {
    const toks = n.split(/\s+/);
    if (toks.length < 3) continue;
    const stripped = toks.filter((t, i) => i === 0 || i === toks.length - 1 || !/^[a-z]\.?$/.test(t)).join(' ');
    if (stripped !== n && nameSet.has(stripped)) union('n:' + stripped, 'n:' + n);
  }

  const byRoot = new Map();
  for (const p of papers) {
    const area = p.Area;
    authorNames(p).forEach((name) => {
      const root = find('n:' + stripAccents(name).toLowerCase());
      let rec = byRoot.get(root);
      if (!rec) { rec = { names: new Map(), papers: 0, areas: new Set(), journals: new Set() }; byRoot.set(root, rec); }
      rec.papers++;
      rec.names.set(name, (rec.names.get(name) || 0) + 1);
      if (area) rec.areas.add(area);
      if (p.Journal) rec.journals.add(p.Journal);
    });
  }
  const out = [];
  for (const [id, rec] of byRoot) {
    const variants = [...rec.names.entries()].sort((a, b) => b[1] - a[1]).map(e => e[0]);
    out.push({
      id,
      Author: variants[0],
      Papers: rec.papers,
      Areas: [...rec.areas].join(', '),
      Journals: [...rec.journals].join(', '),
      Name_Variants: variants.join(';'),
    });
  }
  out.sort((a, b) => (b.Papers - a.Papers) || cmp(a.Author, b.Author) || cmp(a.id, b.id));
  // Across 50 journals the full set would be enormous; keep multi-paper
  // authors (plus everyone in the top slice) so the file stays a sane size.
  const trimmed = out.filter((a, i) => a.Papers >= 2 || i < 5000);
  return trimmed.map(({ id, ...rest }) => rest);
}

function buildAffiliations(papers) {
  const byAff = new Map();
  for (const p of papers) {
    const affs = p.Affiliations ? p.Affiliations.split(';').map(s => s.trim()).filter(Boolean) : [];
    const seen = new Set();
    for (const aff of affs) {
      const key = stripAccents(aff).toLowerCase().replace(/\s+/g, ' ');
      if (seen.has(key)) continue;
      seen.add(key);
      let rec = byAff.get(key);
      if (!rec) { rec = { name: aff, papers: 0, areas: new Set() }; byAff.set(key, rec); }
      rec.papers++;
      if (p.Area) rec.areas.add(p.Area);
    }
  }
  const out = [...byAff.entries()].map(([key, r]) => ({
    key,
    Affiliation: r.name,
    Papers: r.papers,
    Areas: [...r.areas].join(', '),
    Area_Count: r.areas.size,
  }));
  out.sort((a, b) => (b.Papers - a.Papers) || cmp(a.Affiliation, b.Affiliation) || cmp(a.key, b.key));
  return out.slice(0, TOP_AFFILIATIONS).map(({ key, ...rest }) => rest);
}

function buildRecent(papers, registry) {
  const cutoff = new Date(PULL_DATE + 'T00:00:00');
  cutoff.setDate(cutoff.getDate() - RECENT_WINDOW_DAYS);
  const rows = [];
  for (const p of papers) {
    const ds = registry[regKey(p)];
    if (!ds) continue;
    const d = new Date(ds + 'T00:00:00');
    if (isNaN(d) || d < cutoff) continue;
    rows.push({ p, d });
  }
  rows.sort((a, b) => (b.d - a.d) || (b.p._rank - a.p._rank) || cmp(regKey(a.p), regKey(b.p)));
  return rows.slice(0, RECENT_CAP).map(x => ({ ...publicRow(x.p), 'Date Added': registry[regKey(x.p)] }));
}

function publicRow(p) {
  const { _doi, _orcids, _rank, Orcids, ...rest } = p;
  // Persist ORCIDs (pipe-joined, empty slots kept for index alignment with the
  // Authors list) only when at least one is present, so a reused papers file
  // round-trips them back via rehydrateRow. Most rows carry none, so the field
  // is absent from the vast majority and adds no bloat there.
  if (_orcids && _orcids.some(Boolean)) rest.Orcids = _orcids.join('|');
  return rest;
}

// Add forthcoming papers a publisher lists but Crossref hasn't indexed yet (from
// the committed _informs-aia.json). Only DOIs Crossref did not already return
// are added, into their named source, so Crossref silently supersedes the entry
// once it catches up. New rows flow through the registry, so they also appear in
// the "Recently added papers" view.
function mergeSupplement(bySource) {
  const seen = new Set();
  for (const k of Object.keys(bySource)) for (const p of bySource[k] || []) if (p._doi) seen.add(p._doi);
  let added = 0;
  for (const [rawDoi, s] of Object.entries(AIA_SUPPLEMENT)) {
    const doi = (rawDoi || '').toLowerCase();
    if (!doi || seen.has(doi) || !s || !s.Title) continue;
    const src = JOURNALS.find(j => j.key === s.jkey && j.aia);
    if (!src || !bySource[src.key]) continue; // only known, non-retired, advance-publishing sources
    seen.add(doi);
    const year = String(s.Year || PULL_DATE.slice(0, 4));
    const row = {
      Title: stripJats(s.Title),
      Authors: s.Authors || '',
      Affiliations: s.Affiliations || '',
      DOI: 'https://doi.org/' + rawDoi,
      Volume: '', Issue: '', Page: '',
      Year: year,
      Status: 'Articles in Advance',
      Abstract: s.Abstract ? stripJats(s.Abstract) : '',
      'Accepting Editor': s['Accepting Editor'] || '',
      Area: normArea(s.Area || ''),
      Journal: src.name,
      JKey: src.key,
      _doi: doi,
      _orcids: [],
      _rank: pubRank(year, '', '', '', 'Articles in Advance'),
    };
    if (src.seEditors) row['Senior Editor'] = s['Senior Editor'] || '';
    if (src.aeEditors) row['Associate Editor'] = s['Associate Editor'] || '';
    bySource[src.key].push(row);
    added++;
  }
  if (added) console.log(`  merged ${added} forthcoming papers from the advance-articles supplement`);
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`ft50 data build: pull date ${PULL_DATE}${MOCK ? ' (MOCK)' : ''}` +
    `${ONLY.length ? ` (only: ${ONLY.join(', ')})` : ''} — ${JOURNALS.length} journals`);
  await mkdir(DATA_DIR, { recursive: true });

  const bySource = {}; // key -> rows (internal shape)

  // 1. Pull all locally-hosted journals, sequentially (politeness + bounded
  // memory). Sharded journals are pulled by their own satellite pipelines.
  for (const src of LOCAL_JOURNALS) {
    console.log(`${src.name} (${src.issns.join(', ')}):`);
    bySource[src.key] = await pullJournal(src);
    console.log(`  ${src.key}: ${bySource[src.key].length} papers`);
    if (!MOCK) await sleep(JOURNAL_PAUSE_MS);
  }
  await applyInformsEditors(bySource);
  mergeSupplement(bySource);

  // 2. Journals dropped from the FT50 list: their data files are removed and
  // they disappear from the manifest (check-ft50-list.mjs marks them retired).
  for (const src of RETIRED) {
    const path = join(DATA_DIR, `papers-${src.key}.json`);
    if (existsSync(path)) {
      await rm(path);
      console.log(`  ${src.key}: retired from the FT50 list — removed ${`papers-${src.key}.json`}`);
    }
  }

  // 3. Deterministic order per source, then combined order for aggregates.
  for (const src of LOCAL_JOURNALS) {
    bySource[src.key].sort((a, b) => (b._rank - a._rank) || cmp(regKey(a), regKey(b)));
  }
  const allPapers = LOCAL_JOURNALS.flatMap(s => bySource[s.key]);

  // Pre-print (arXiv/SSRN) open-access links — cached + incremental, exactly
  // like the fun/lit native pipeline. Kept non-fatal so a slow/failed OpenAlex
  // run never aborts the data build; the cache we already have still applies.
  // The heavy title-search backfill runs in its own scheduled workflow via
  // preprints-ci.mjs; the in-build pass here is strictly time-boxed.
  const preprintCache = await loadJsonIfExists(join(DATA_DIR, '_preprints.json'), {});
  try { await resolvePreprints(allPapers, preprintCache); }
  catch (e) { console.warn('  preprints resolve failed (non-fatal):', e.message); }
  if (!MOCK) await writeJson('_preprints.json', preprintCache);
  applyPreprints(allPapers, preprintCache);

  const reg = await loadRegistry();
  const registry = updateRegistry(bySource, reg);

  const authors = buildAuthors(allPapers);
  const affiliations = buildAffiliations(allPapers);
  const recent = buildRecent(allPapers, registry);

  // 4. Write per-journal paper files + manifest (capability flags included so
  // the page can adapt its filters per journal without hardcoding keys).
  const sources = [];
  let total = 0;
  for (const src of LOCAL_JOURNALS) {
    const rows = bySource[src.key].map(publicRow);
    const file = `papers-${src.key}.json`;
    await writeJson(file, rows);
    total += rows.length;
    let firstYear = 0;
    for (const r of rows) {
      const y = parseInt(r.Year, 10);
      if (y && (!firstYear || y < firstYear)) firstYear = y;
    }
    const entry = { key: src.key, name: src.name, short: src.short || src.name, publisher: src.publisher, file, count: rows.length };
    if (firstYear) entry.firstYear = firstYear;
    if (src.url) entry.url = src.url;
    if (src.editors) entry.editors = true;
    if (src.seEditors) entry.seEditors = true;
    if (src.aeEditors) entry.aeEditors = true;
    if (src.aia) entry.aia = true;
    if (src.limitedCoverage) entry.limitedCoverage = true;
    // Journals carried for another list (e.g. UTD24's INFORMS Journal on
    // Computing) but NOT on the FT50 list: the page must not count them as
    // FT50, and the yearly FT-list check must not retire them.
    if (src.notFT) entry.notFT = true;
    // The journal's ABS/AJG 2024 grade — the lit page derives its ABS 4/4*
    // and ABS 3 journal-type buckets (and card badges) from this.
    if (src.abs) entry.abs = src.abs;
    sources.push(entry);
  }

  // Sharded journals: manifest entry only (their papers file is built and
  // committed by the satellite repo's own pipeline; the page fetches
  // entry.base + entry.file — GitHub Pages sends Access-Control-Allow-Origin:
  // * so a cross-origin data site works). No count: the satellite owns it.
  for (const src of SHARDED_JOURNALS) {
    const entry = { key: src.key, name: src.name, short: src.short || src.name, publisher: src.publisher, file: `papers-${src.key}.json`, base: src.base };
    if (src.aia) entry.aia = true;
    if (src.limitedCoverage) entry.limitedCoverage = true;
    if (src.notFT) entry.notFT = true;
    // The journal's ABS/AJG 2024 grade — the lit page derives its ABS 4/4*
    // and ABS 3 journal-type buckets (and card badges) from this.
    if (src.abs) entry.abs = src.abs;
    sources.push(entry);
  }

  const meta = {
    lastPull: PULL_DATE,
    paperCount: total,
    journalCount: JOURNALS.length,
    perSource: Object.fromEntries(sources.map(s => [s.key, s.count])),
    source: 'Crossref REST API, one pull per journal (ABS 4/4* journals beyond the FT50/UTD24 lists)',
  };

  await writeJson('sources.json', sources);
  await writeJson('authors.json', authors);
  await writeJson('affiliations.json', affiliations);
  await writeJson('recent.json', recent);
  await writeJson('meta.json', meta);
  await writeJson('_registry.json', registry);

  console.log(`done: ${total} papers (${sources.map(s => `${s.key}:${s.count}`).join(' ')}), ` +
    `${authors.length} authors, ${affiliations.length} affiliations, ${recent.length} recent`);
}

async function writeJson(name, data) {
  // Minified + deterministic: unchanged data produces identical bytes and
  // therefore no needless git commit.
  await writeFile(join(DATA_DIR, name), JSON.stringify(data), 'utf8');
}

// Only run when executed directly — importing a helper from this module for a
// test must not fire the whole network pipeline.
if (process.argv[1] && import.meta.url === (await import('node:url')).pathToFileURL(process.argv[1]).href) {
  main().catch(e => { console.error(e); process.exit(1); });
}
