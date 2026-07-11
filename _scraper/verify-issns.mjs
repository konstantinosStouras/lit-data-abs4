/*
 * verify-issns.mjs — sanity-check _scraper/journals.json against Crossref.
 * For every non-retired journal, queries Crossref's journal registry by ISSN
 * and compares titles, so a typo'd ISSN (which would silently harvest the
 * wrong journal or nothing) is caught in the workflow log. Warn-only: title
 * spelling differs across registries, so mismatches need a human eye.
 * Usage: node verify-issns.mjs
 */
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const MAILTO = process.env.FT50_MAILTO || 'kstouras@gmail.com';
const journals = JSON.parse(await readFile(join(__dirname, 'journals.json'), 'utf8'));
const norm = (s) => String(s || '').toLowerCase().replace(/&/g, ' and ').replace(/^the\s+/, '').replace(/[^a-z0-9]+/g, ' ').trim();
let warned = 0;
for (const j of journals) {
  if (j.retired) continue;
  const issn = (j.issns || [])[0];
  if (!issn) { console.log(`::warning::${j.key}: no ISSN`); warned++; continue; }
  try {
    const r = await fetch(`https://api.crossref.org/journals/${issn}?mailto=${MAILTO}`);
    if (!r.ok) { console.log(`::warning::${j.key}: Crossref has no journal for ISSN ${issn} (HTTP ${r.status})`); warned++; continue; }
    const title = (await r.json()).message.title || '';
    const ok = norm(title) === norm(j.name) || norm(title).includes(norm(j.name)) || norm(j.name).includes(norm(title));
    console.log(`${ok ? 'ok ' : '?? '} ${j.key}: "${j.name}" -> Crossref "${title}" (${issn})`);
    if (!ok) { console.log(`::warning::${j.key}: title mismatch — check the ISSN`); warned++; }
  } catch (e) {
    console.log(`::warning::${j.key}: check failed (${e.message})`);
  }
  await new Promise((res) => setTimeout(res, 250));
}
console.log(warned ? `${warned} warning(s) — review above.` : 'All ISSNs check out.');
