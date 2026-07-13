/*
 * citations-ci.mjs — the ONLINE citation-count sweep for this shard's dataset
 * (data/), run on GitHub Actions by .github/workflows/citations-update.yml
 * (daily). Near-verbatim from the reference fun/lit/_scraper/citations-ci.mjs
 * in the konstantinosStouras.github.io repo — see its header for the full
 * design: a bounded oldest-check-first sweep of OpenAlex cited_by_count
 * (50 DOIs/call) + Semantic Scholar citationCount (500 DOIs/POST) into the
 * incremental data/_citations.json, then the cache is applied to the
 * papers-*.json files (CitedBy = max of Crossref/OpenAlex/Semantic Scholar,
 * CitedBySrc naming the winner).
 *
 * Flags (used by the workflow's push-retry path):
 *   --apply-only              skip the sweep; just apply the cache.
 *   --merge-cache=<file>      overlay this cache file's entries first (the
 *                             more recently checked entry wins; same-day ties
 *                             go to the higher count).
 *
 * Env: FT50_DATA_DIR (data dir override, used by tests), FT50_MAILTO
 * (OpenAlex contact; the workflow pins this shard's own quota identity),
 * FT50_CITATIONS_BACKFILL_MS, FT50_CITATIONS_BACKFILL_CAP,
 * FT50_CITATIONS_MIN_AGE (days a cache entry stays fresh, default 2).
 * ===========================================================================
 */

import { readFile, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { applyCitations, refreshCitations } from './build-data.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA = process.env.FT50_DATA_DIR || join(__dirname, '..', 'data');

const args = process.argv.slice(2);
const applyOnly = args.includes('--apply-only');
const mergeArg = args.find(a => a.startsWith('--merge-cache='));

async function loadJson(path, fallback) {
  if (!existsSync(path)) return fallback;
  try { return JSON.parse(await readFile(path, 'utf8')); } catch { return fallback; }
}

const sources = await loadJson(join(DATA, 'sources.json'), []);
const cache = await loadJson(join(DATA, '_citations.json'), {});

// Overlay a saved cache from an interrupted/raced run: the more recently
// checked entry wins; on a same-day tie the higher count does.
if (mergeArg) {
  const ours = await loadJson(mergeArg.split('=').slice(1).join('='), {});
  let merged = 0;
  for (const [doi, e] of Object.entries(ours)) {
    const cur = cache[doi];
    if (cur && ((cur.t || 0) > (e.t || 0) ||
        ((cur.t || 0) === (e.t || 0) && (cur.c || 0) >= (e.c || 0)))) continue;
    cache[doi] = e; merged++;
  }
  console.log(`merged ${merged} cache entr(ies) from ${mergeArg.split('=')[1]}`);
}

const filesByKey = {};
const all = [];
for (const s of sources) {
  const rows = await loadJson(join(DATA, s.file), []);
  if (!Array.isArray(rows) || !rows.length) continue;
  filesByKey[s.key] = rows;
  for (const r of rows) {
    r._doi = (r.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
    all.push(r);
  }
}
console.log(`loaded ${all.length} papers from ${Object.keys(filesByKey).length} source(s); ` +
  `${Object.keys(cache).length} cache entries`);

if (!applyOnly) {
  const checkpoint = (c) => writeFile(join(DATA, '_citations.json'), JSON.stringify(c), 'utf8');
  await refreshCitations(all, cache, {
    cap: parseInt(process.env.FT50_CITATIONS_BACKFILL_CAP || '500000', 10),
    budgetMs: parseInt(process.env.FT50_CITATIONS_BACKFILL_MS || String(45 * 60 * 1000), 10),
    minAgeDays: parseInt(process.env.FT50_CITATIONS_MIN_AGE || '2', 10),
    log: true,
    checkpoint,
  });
}

// Apply the cache to the rows and write everything back.
applyCitations(all, cache);
let withCount = 0;
for (const [key, rows] of Object.entries(filesByKey)) {
  for (const r of rows) {
    if (r.CitedBy > 0) withCount++;
    delete r._doi;
  }
  const s = sources.find(x => x.key === key);
  await writeFile(join(DATA, s.file), JSON.stringify(rows), 'utf8');
}
await writeFile(join(DATA, '_citations.json'), JSON.stringify(cache), 'utf8');

const today = Math.floor(Date.now() / 86400000);
const minAge = parseInt(process.env.FT50_CITATIONS_MIN_AGE || '2', 10);
const stale = new Set();
for (const r of all) {
  const doi = (r.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
  if (!doi || stale.has(doi)) continue;
  const c = cache[doi];
  if (!c || (c.t || 0) <= today - minAge) stale.add(doi);
}
console.log(`${withCount} papers carry a citation count; stale/unchecked DOIs remaining: ${stale.size}`);
