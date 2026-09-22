/*
 * VENDORED from the site repo's lit/_scraper/_chunked-json.mjs — keep in sync,
 * like _entities.mjs and _titlecase-lexicon.mjs. It exists here because this
 * shard's _api-abstracts.json outgrew a single committed file: see the header
 * of abstracts-ci.mjs and the site repo's CLAUDE.md.
 */
/*
 * Chunked JSON files — keep every committed data file under GitHub's hard
 * 100 MiB push limit.
 * ===================================================================
 *
 * WHY THIS EXISTS (the Aug 2026 outage): GitHub's pre-receive hook rejects any
 * push containing a file ≥ 100 MiB. On 2026-08-01 lit/data-workingpapers/
 * papers-wp-arxiv.json crossed it, and from then on EVERY working-papers
 * crawl that found new arXiv rows had its push rejected — the archive froze
 * while meta.json's lastPull kept advancing (only no-progress runs could still
 * push). Days later lit/data-refs/_refs-cache.json hit the same wall and the
 * citation-graph backfill started failing identically. The durable fix is to
 * never let a single committed file approach the limit: a logical JSON file is
 * written as one or more PART files, each safely under a byte cap.
 *
 * Naming: the first part keeps the original name (so every existing reader and
 * URL stays valid), later parts insert -N before .json:
 *   papers-wp-arxiv.json, papers-wp-arxiv-2.json, papers-wp-arxiv-3.json, …
 *   _refs-cache.json, _refs-cache-2.json, …
 *
 * Reading merges the parts back into one value: arrays concatenate, objects
 * merge (parts never share keys — the writer splits, it doesn't duplicate).
 * A dataset written before chunking existed is simply a single part, so
 * readers are fully backward-compatible; a small dataset writes a single part,
 * so writers are too. writeChunkedJson also deletes stale higher-numbered
 * parts left over from an earlier, larger split, so a shrinking dataset can
 * never resurrect old rows.
 *
 * The default cap (48 MiB) sits far under the 100 MiB hard limit AND under
 * GitHub's 50 MB "recommended maximum" warning, leaving room for a single
 * run's growth between size checks.
 */

import { existsSync, readFileSync, unlinkSync } from 'node:fs';
import { readFile, writeFile, rename, unlink } from 'node:fs/promises';

export const CHUNK_CAP_BYTES = 48 * 1024 * 1024;

// The i-th part's path (1-based): part 1 is the file itself.
export function chunkPartPath(file, i) {
  return i <= 1 ? file : String(file).replace(/\.json$/i, '') + '-' + i + '.json';
}

function mergePart(out, part) {
  if (out === null) return part;
  if (Array.isArray(out) && Array.isArray(part)) return out.concat(part);
  if (!Array.isArray(out) && out && typeof out === 'object' && part && typeof part === 'object' && !Array.isArray(part)) {
    return Object.assign(out, part);
  }
  return out; // shape mismatch — ignore the stray part rather than corrupt
}

export async function readChunkedJson(file, fallback) {
  let out = null;
  for (let i = 1; ; i++) {
    const p = chunkPartPath(file, i);
    if (!existsSync(p)) break;
    let part;
    try { part = JSON.parse(await readFile(p, 'utf8')); } catch { break; }
    out = mergePart(out, part);
  }
  return out === null ? fallback : out;
}

export function readChunkedJsonSync(file, fallback) {
  let out = null;
  for (let i = 1; ; i++) {
    const p = chunkPartPath(file, i);
    if (!existsSync(p)) break;
    let part;
    try { part = JSON.parse(readFileSync(p, 'utf8')); } catch { break; }
    out = mergePart(out, part);
  }
  return out === null ? fallback : out;
}

// Serialize `data` (an array or a plain object) into part-payload strings,
// each ≤ cap bytes unless a single element alone exceeds it. Matches
// JSON.stringify semantics: an undefined/function array element serializes as
// null; an undefined/function object value drops its key.
export function splitJsonPayloads(data, cap = CHUNK_CAP_BYTES) {
  const isArr = Array.isArray(data);
  const pieces = [];
  if (isArr) {
    for (const v of data) pieces.push(JSON.stringify(v) ?? 'null');
  } else {
    for (const [k, v] of Object.entries(data || {})) {
      const sv = JSON.stringify(v);
      if (sv !== undefined) pieces.push(JSON.stringify(k) + ':' + sv);
    }
  }
  const open = isArr ? '[' : '{', close = isArr ? ']' : '}';
  const parts = [];
  let cur = [], curBytes = 2; // brackets
  for (const piece of pieces) {
    const b = Buffer.byteLength(piece, 'utf8') + 1; // + separating comma
    if (cur.length && curBytes + b > cap) { parts.push(cur); cur = []; curBytes = 2; }
    cur.push(piece);
    curBytes += b;
  }
  parts.push(cur); // an empty dataset still writes its (empty) first part
  return parts.map((p) => open + p.join(',') + close);
}

// Atomic per-part write (temp + rename, the same pattern as the pipelines'
// writeJson helpers), then stale-part cleanup. Returns the written file paths
// in part order.
export async function writeChunkedJson(file, data, cap = CHUNK_CAP_BYTES) {
  const payloads = splitJsonPayloads(data, cap);
  const files = [];
  for (let i = 0; i < payloads.length; i++) {
    const dest = chunkPartPath(file, i + 1);
    const tmp = `${dest}.tmp-${process.pid}`;
    await writeFile(tmp, payloads[i], 'utf8');
    let renamed = false;
    for (let a = 0; a < 10 && !renamed; a++) {
      try { await rename(tmp, dest); renamed = true; }
      catch (e) {
        if (!['EPERM', 'EBUSY', 'EACCES', 'ENOTEMPTY', 'EEXIST'].includes(e.code)) throw e;
        await new Promise((r) => setTimeout(r, 200 * (a + 1)));
      }
    }
    if (!renamed) await writeFile(dest, payloads[i], 'utf8');
    files.push(dest);
  }
  // A re-write that needs fewer parts must remove the leftovers, or a reader
  // would merge stale data back in.
  for (let i = payloads.length + 1; ; i++) {
    const p = chunkPartPath(file, i);
    if (!existsSync(p)) break;
    try { unlinkSync(p) } catch { try { await unlink(p); } catch { /* next run retries */ } }
  }
  return files;
}
