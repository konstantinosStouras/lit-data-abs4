/*
 * preprints-ci.mjs — the ONLINE pre-print backfill for this dataset.
 * ===========================================================================
 * Incrementally links each paper's free arXiv/SSRN pre-print by OpenAlex
 * title-search, straight from the committed dataset — adapted from
 * fun/lit/_scraper/preprints-ci.mjs (see that header for the full story).
 * Driven by .github/workflows/preprints-backfill.yml, which runs it a few times a day and commits the
 * refreshed data back. Newest papers are searched first.
 *
 * Each run is bounded (FT50_PREPRINT_BACKFILL_MS, default 25 min of
 * searching) and quota-aware: OpenAlex rate-limits are waited out politely,
 * and when it signals the DAY's request quota is spent the run saves
 * progress and exits cleanly — the next scheduled run resumes from the
 * _preprints.json cache. The workflow pins FT50_MAILTO to this dataset's own
 * OpenAlex quota identity so parallel backfills never starve each other.
 *
 * Flags (used by the workflow's push-retry path):
 *   --apply-only              skip searching; just apply the cache to the
 *                             papers-*.json files and write them.
 *   --merge-cache=<file>      overlay this cache file's entries onto the
 *                             repo's _preprints.json first (a FOUND link is
 *                             never downgraded), so a rejected push can
 *                             re-apply this run's finds on a fresher dataset.
 * ===========================================================================
 */

import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { searchPreprintsByTitle } from './build-data.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA = process.env.FT50_DATA_DIR || join(__dirname, '..', 'data');

// A never-built dataset has no data dir yet; the checkpoint and final
// writes below must not crash the scheduled run over that.
await mkdir(DATA, { recursive: true });

const args = process.argv.slice(2);
const applyOnly = args.includes('--apply-only');
const mergeArg = args.find(a => a.startsWith('--merge-cache='));

async function loadJson(path, fallback) {
  if (!existsSync(path)) return fallback;
  try { return JSON.parse(await readFile(path, 'utf8')); } catch { return fallback; }
}

const sources = await loadJson(join(DATA, 'sources.json'), []);
const cache = await loadJson(join(DATA, '_preprints.json'), {});

// Overlay a saved cache from an interrupted/raced run. A found link ({u})
// already in the repo cache is never downgraded by the overlay.
if (mergeArg) {
  const ours = await loadJson(mergeArg.split('=').slice(1).join('='), {});
  let merged = 0;
  for (const [doi, entry] of Object.entries(ours)) {
    if (cache[doi] && cache[doi].u) continue;
    cache[doi] = entry; merged++;
  }
  console.log(`merged ${merged} cache entr(ies) from ${mergeArg.split('=')[1]}`);
}

// Sharded manifest entries (a `base` URL) have no local papers file — skip.
const filesByKey = {};
const all = [];
for (const s of sources) {
  if (s.base) continue;
  const rows = await loadJson(join(DATA, s.file), []);
  if (!Array.isArray(rows) || !rows.length) continue;
  filesByKey[s.key] = rows;
  for (const r of rows) {
    r._doi = (r.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
    r.JKey = r.JKey || s.key;
    all.push(r);
  }
}
console.log(`loaded ${all.length} papers from ${Object.keys(filesByKey).length} source(s); ` +
  `${Object.keys(cache).length} cache entries`);

if (!applyOnly) {
  // patient:true rides out per-second throttling (and exits cleanly on daily-
  // quota exhaustion); budgetMs bounds the run so the job always finishes.
  const found = await searchPreprintsByTitle(all, cache, {
    cap: parseInt(process.env.FT50_PREPRINT_BACKFILL_CAP || '100000', 10),
    budgetMs: parseInt(process.env.FT50_PREPRINT_BACKFILL_MS || String(25 * 60 * 1000), 10),
    sleepMs: 300,
    patient: true,
    log: true,
    checkpoint: (c) => writeFile(join(DATA, '_preprints.json'), JSON.stringify(c), 'utf8'),
  });
  console.log(`linked ${found} new pre-print(s) this run`);
}

// Apply the cache to the rows and write everything back.
let withLink = 0;
for (const [key, rows] of Object.entries(filesByKey)) {
  for (const r of rows) {
    const x = cache[r._doi];
    if (x && x.u) { r.Preprint = x.u; r.PreprintSrc = x.s; withLink++; }
    delete r._doi;
  }
  const s = sources.find(x => x.key === key);
  await writeFile(join(DATA, s.file), JSON.stringify(rows), 'utf8');
}
await writeFile(join(DATA, '_preprints.json'), JSON.stringify(cache), 'utf8');

const remaining = all.filter(p => {
  const doi = (p.DOI || '').replace(/^https?:\/\/doi\.org\//i, '').toLowerCase();
  const c = cache[doi];
  return doi && c && c.none && !c.ts && parseInt(p.Year, 10) >= 2005;
}).length;
console.log(`${withLink} papers carry a pre-print link; backlog remaining: ${remaining}`);
