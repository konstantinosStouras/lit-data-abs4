/*
 * clean-junk-abstracts.mjs — one-off/maintenance cleanup over a committed data
 * directory: blank every served "abstract" that is not the paper's abstract.
 * ===========================================================================
 * The pipelines now drop, at ingest, a deposited "abstract" that is really an
 * editorial PLAIN-LANGUAGE SUMMARY (INFORMS' Operations Research / IJOC blurbs
 * — "In '<Title>', <the authors> develop…", third person, naming the paper's
 * own authors; user report 2026-08) or a bare CITATION-LINE STUB (AEA's
 * "<Title> by <Authors>. Published in volume …", the JSTOR/OUP-era
 * "<Authors>, <Title>, <Journal>, Vol. …, pp. …") — junkAbstract in
 * _entities.mjs (VENDORED from the site repo — keep in sync), applied in the
 * build pipeline and the API abstracts backfill.
 * This CLI applies the SAME rules to an already-committed dataset, for the
 * back-catalogue harvested before the guard existed.
 *
 * It rewrites each papers-*.json (Abstract → '' on a junk row; the row then
 * counts as "needy" for the pubsonline harvest / FT50 API backfill, which fill
 * the real text), refreshes recent.json (full row copies), and HEALS the
 * abstract caches sitting in the same directory: a junk `_api-abstracts.json`
 * entry is re-stamped a TTL miss (re-resolved under the new guard) and a junk
 * `_informs-abstracts.json` entry is deleted (re-crawled from the page).
 * HBR / MIT Sloan Management Review rows are exempt — practitioner pieces have
 * no author abstract, so the deposited third-person deck stays.
 *
 *   node _scraper/clean-junk-abstracts.mjs               # this repo's data/
 *   ... --dry-run          # report only, write nothing
 * ===========================================================================
 */
import { readFileSync, writeFileSync, readdirSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { junkAbstract } from './_entities.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const argOf = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : null;
};
const DIR = resolve(argOf('--dir') || join(__dirname, '..', 'data'));
const DRY = args.includes('--dry-run');
const day = () => Math.floor(Date.now() / 86400000);

// Practitioner journals whose third-person deck IS the journal's summary text.
const EXEMPT_JKEYS = new Set(['hbr', 'smr']);

const rowCtx = (r) => ({ title: r.Title, authors: r.Authors, journal: r.Journal });

// Returns the junk kind ('' when the row is fine or exempt).
function junkKindOf(r) {
  if (!r || typeof r.Abstract !== 'string' || !r.Abstract) return '';
  if (EXEMPT_JKEYS.has(r.JKey)) return '';
  return junkAbstract(r.Abstract, rowCtx(r));
}

if (!existsSync(DIR)) {
  console.error(`clean-junk-abstracts: no such directory: ${DIR}`);
  process.exit(1);
}

const files = readdirSync(DIR).filter((f) => /^papers-.*\.json$/.test(f)).sort();
if (!files.length) console.log(`clean-junk-abstracts: no papers-*.json in ${DIR}`);

let totSummary = 0, totStub = 0, totRows = 0;
const examples = [];
const ctxByDoi = new Map(); // bare doi -> row ctx, for the cache heals below

const bareDoi = (r) => String(r.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();

for (const f of files) {
  const rows = JSON.parse(readFileSync(join(DIR, f), 'utf8'));
  if (!Array.isArray(rows)) continue;
  let nSum = 0, nStub = 0;
  for (const r of rows) {
    const doi = bareDoi(r);
    if (doi && !ctxByDoi.has(doi) && !EXEMPT_JKEYS.has(r.JKey)) ctxByDoi.set(doi, rowCtx(r));
    const kind = junkKindOf(r);
    if (!kind) continue;
    if (kind === 'summary') nSum++; else nStub++;
    if (examples.length < 10) {
      examples.push(`${f} [${kind}] ${doi}: ${JSON.stringify(String(r.Abstract).slice(0, 90))}…`);
    }
    r.Abstract = '';
  }
  totRows += rows.length;
  totSummary += nSum; totStub += nStub;
  if (nSum || nStub) {
    if (!DRY) writeFileSync(join(DIR, f), JSON.stringify(rows), 'utf8');
    console.log(`  ${f}: ${nSum} summary + ${nStub} citation-stub abstract(s) blanked`);
  }
}

// recent.json carries full row copies — keep it consistent with the papers files.
const recentPath = join(DIR, 'recent.json');
if (existsSync(recentPath)) {
  const recent = JSON.parse(readFileSync(recentPath, 'utf8'));
  if (Array.isArray(recent)) {
    let n = 0;
    for (const r of recent) {
      if (junkKindOf(r)) { r.Abstract = ''; n++; }
    }
    if (n) {
      if (!DRY) writeFileSync(recentPath, JSON.stringify(recent), 'utf8');
      console.log(`  recent.json: ${n} row(s) blanked`);
    }
  }
}

// Heal the API cache: a junk entry would re-apply on the next backfill run,
// so re-stamp it a TTL miss (the guarded legs then re-resolve it for real).
const apiCachePath = join(DIR, '_api-abstracts.json');
if (existsSync(apiCachePath)) {
  const raw = JSON.parse(readFileSync(apiCachePath, 'utf8'));
  const map = raw.map || raw;
  let n = 0;
  for (const [doi, rec] of Object.entries(map)) {
    if (!rec || !rec.a) continue;
    const ctx = ctxByDoi.get(doi);
    if (ctx && junkAbstract(rec.a, ctx)) { map[doi] = { none: 1, t: day() }; n++; }
  }
  if (n) {
    if (!DRY) writeFileSync(apiCachePath, JSON.stringify(map), 'utf8');
    console.log(`  _api-abstracts.json: ${n} junk entr${n === 1 ? 'y' : 'ies'} re-stamped as misses`);
  }
}

// Heal the pubsonline cache: a junk capture is deleted so the crawler re-reads
// the page under the guarded extractor (same convention as the chrome heal).
const pubCachePath = join(DIR, '_informs-abstracts.json');
if (existsSync(pubCachePath)) {
  const raw = JSON.parse(readFileSync(pubCachePath, 'utf8'));
  const map = raw.map || raw;
  let n = 0;
  for (const [doi, rec] of Object.entries(map)) {
    if (!rec || !rec.a) continue;
    const ctx = ctxByDoi.get(doi);
    if (ctx && junkAbstract(rec.a, ctx)) { delete map[doi]; n++; }
  }
  if (n) {
    if (!DRY) writeFileSync(pubCachePath, JSON.stringify(map), 'utf8');
    console.log(`  _informs-abstracts.json: ${n} junk entr${n === 1 ? 'y' : 'ies'} deleted (re-crawl)`);
  }
}

if (examples.length) {
  console.log('\nexamples:');
  for (const e of examples) console.log('  ' + e);
}
console.log(`\n${DRY ? '[dry-run] ' : ''}${DIR}: ${totSummary} plain-language summar${totSummary === 1 ? 'y' : 'ies'} + ${totStub} citation stub(s) blanked over ${totRows} rows.`);
