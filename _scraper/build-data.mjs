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
