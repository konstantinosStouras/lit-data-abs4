/*
 * abstracts-selftest.mjs — offline tests (no network) for this shard's API
 * abstract backfill (abstracts-ci.mjs, vendored from the site repo's
 * lit/_scraper-ft50/ — keep in sync): OpenAlex inverted-index
 * reconstruction, the cache-merge rule, the needy-row test, the Elsevier /
 * Springer / Scopus parsers, the credential-tier miss rules, a source pin
 * that the institutional token only ever travels as a request header, and
 * five whole-run scenarios driven against a stubbed fetch in a child process
 * (the module reads its credentials and data dir at import time, so each
 * scenario is its own process).
 * Run: node _scraper/abstracts-selftest.mjs
 */
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { invertedToText, mergeAbsCache, isNeedy, elsevierAbstract, springerAbstract, shouldStampMiss,
  credentialTier, missIsFresh, missStamp, elsAnswerIsDefinitive, scopusDoiQuery, scopusAbstracts,
  readRateLimit, elsErrorText, isRecentYear, headerSafeValue, ELS_PREFIX, SPR_PREFIX, main, betterAbstract } from './abstracts-ci.mjs';
import { readChunkedJsonSync } from './_chunked-json.mjs';

const SELF = fileURLToPath(import.meta.url);
const HERE = dirname(SELF);
let fails = 0;
const ok = (c, m) => { if (c) console.log(`  ✓ ${m}`); else { console.error(`  ✗ ${m}`); fails++; } };
const eq = (g, w, m) => ok(g === w, `${m}${g === w ? '' : `  (got ${JSON.stringify(g)}, want ${JSON.stringify(w)})`}`);

// ── Whole-run scenarios (child-process side) ────────────────────────────────
// The parent spawns this same file with ABS_SCENARIO=<name>, the credentials
// the scenario needs and FT50_DATA_DIR pointing at a scratch data dir; this
// branch stubs fetch, runs main() and asserts on the cache + papers files.
if (process.env.ABS_SCENARIO) {
  const name = process.env.ABS_SCENARIO;
  const DIR = process.env.FT50_DATA_DIR;
  const today = Math.floor(Date.now() / 86400000);
  const NOW_YEAR = new Date().getUTCFullYear();
  const E1 = '10.1016/j.ejor.2026.01.001', E2 = '10.1016/j.ejor.2026.01.002', E3 = '10.1016/j.ejor.2026.01.003';
  const E4 = '10.1016/j.ejor.2026.01.004', E5 = '10.1016/s0377-2217(99)00123-4', E6 = '10.1016/j.ejor.2020.01.006';
  const E7 = '10.1016/j.ejor.2021.01.007', E8 = '10.1016/j.ejor.2020.01.008', E9 = '10.1016/j.ejor.2026.01.009';
  const E10 = '10.1016/j.ejor.2015.01.010';
  const O1 = '10.1093/qje/qjaa001', O2 = '10.1093/rfs/hhaa002', S1 = '10.1007/s11002-024-09999-9';
  // ≥ 200 chars: betterAbstract only replaces an existing teaser with a
  // candidate that is a real paragraph AND materially longer.
  const TXT = (s) => `${s} This is a real abstract of the paper, long enough to pass the sixty-character floor of every leg and the two-hundred-character floor the upgrade rule applies before it will replace an existing one-line teaser on a row.`;
  const calls = [];
  const seenHeaders = [];
  const json = (body, status = 200, headers = {}) => new Response(JSON.stringify(body), {
    status, headers: { 'content-type': 'application/json', ...headers } });
  const scopusEntries = (dois, serve) => dois.map(d => serve[d] === undefined ? null
    : { '@_fa': 'true', 'prism:doi': serve[d].doiAs || d.toUpperCase(), ...(serve[d].text ? { 'dc:description': serve[d].text } : {}) }).filter(Boolean);
  const EMPTY = { 'search-results': { 'opensearch:totalResults': '0', entry: [{ '@_fa': 'true', error: 'Result set was empty' }] } };
  const baseRows = () => [
    { DOI: E1, Title: 'Paper E1', Authors: 'A. Author, B. Author', Journal: 'EJOR', JKey: 'ejor', Year: String(NOW_YEAR), Abstract: '' },
    { DOI: E2, Title: 'Paper E2', Authors: 'C. Author', Journal: 'EJOR', JKey: 'ejor', Year: String(NOW_YEAR), Abstract: 'Teaser only.' },
    { DOI: E3, Title: 'Paper E3', Authors: 'D. Author', Journal: 'EJOR', JKey: 'ejor', Year: String(NOW_YEAR), Abstract: '' },
    { DOI: E4, Title: 'Paper E4', Authors: 'E. Author', Journal: 'EJOR', JKey: 'ejor', Year: String(NOW_YEAR), Abstract: '' },
    { DOI: E5.toUpperCase(), Title: 'Paper E5', Authors: 'F. Author', Journal: 'EJOR', JKey: 'ejor', Year: '1999', Abstract: '' },
    { DOI: E6, Title: 'Paper E6', Authors: 'G. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2020', Abstract: '' },
    { DOI: E7, Title: 'Paper E7', Authors: 'H. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2021', Abstract: '' },
    { DOI: E8, Title: 'Paper E8', Authors: 'M. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2020', Abstract: '' },
    { DOI: E9, Title: 'Paper E9', Authors: 'N. Author', Journal: 'EJOR', JKey: 'ejor', Year: String(NOW_YEAR), Abstract: '' },
    { DOI: E10, Title: 'Paper E10', Authors: 'P. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2015', Abstract: '' },
    { DOI: '10.1016/j.ejor.2019.01.099', Title: 'Paper E99', Authors: 'I. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2019', Abstract: 'x'.repeat(400) },
    { DOI: O1, Title: 'Paper O1', Authors: 'J. Author', Journal: 'QJE', JKey: 'qje', Year: String(NOW_YEAR), Abstract: '' },
    { DOI: `https://doi.org/${O2}`, Title: 'Paper O2', Authors: 'K. Author', Journal: 'RFS', JKey: 'rfs', Year: String(NOW_YEAR), Abstract: '' },
    { DOI: S1, Title: 'Paper S1', Authors: 'L. Author', Journal: 'MkLett', JKey: 'mlet', Year: '2024', Abstract: '' },
  ];
  // N old-year Elsevier rows, for the scenarios that need whole batches of them.
  const oldRows = (n) => Array.from({ length: n }, (_, i) => ({
    DOI: `10.1016/j.ejor.2015.02.${String(i + 1).padStart(3, '0')}`, Title: `Old paper ${i + 1}`, Authors: `Q${i}. Author`,
    Journal: 'EJOR', JKey: 'ejor', Year: '2015', Abstract: '' }));
  const perDoiUrls = () => calls.filter(u => u.includes('/content/abstract/doi/'));
  const scopusUrls = () => calls.filter(u => u.includes('/content/search/scopus'));
  const springerUrls = () => calls.filter(u => u.includes('api.springernature.com'));
  const SPR2 = '10.1007/s11002-024-09999-2', SPR3 = '10.1007/s11002-024-09999-3';
  const CHROME = 'Previous articleNext article No AccessSome Paper TitleSome AuthorPDFPDF PLUS Add to favoritesDownload CitationTrack CitationsPermissionsReprints Share onFacebookXLinkedIn';
  let openalexStatus = 200, openalexDelayMs = 0;
  const scen = {
    // The full token run: Scopus serves the bulk (and settles a record without
    // text), the per-DOI leg the rest — proving the token unlocks text first.
    'token': {
      cache: { [E5]: { none: 1, t: today - 1 }, [E6]: { none: 1, t: today - 1, k: 2 }, [E7]: { none: 1, t: today - 1, k: 1 } },
      scopus: (dois) => json({ 'search-results': { 'opensearch:totalResults': '3',
        entry: scopusEntries(dois, { [E1]: { text: TXT('Scopus E1.') }, [E8]: {}, [E5]: { text: TXT('Scopus E5.'), doiAs: '10.1016/S0377-2217(99)00123-4' } }) } },
        200, { 'X-RateLimit-Limit': '20000', 'X-RateLimit-Remaining': '19990', 'X-RateLimit-Reset': '1789000000' }),
      abstract: (doi) => doi === E2 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:description': TXT('Retrieval E2.') } } }, 200, { 'X-RateLimit-Remaining': '9990' })
        : doi === E3 || doi === E7 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:title': 'no description' } } }, 200, { 'X-RateLimit-Remaining': '9989' })
        : doi === E4 ? json({ 'service-error': { status: { statusCode: 'GENERIC_ERROR' } } }, 503)
        : json({ 'service-error': { status: { statusCode: 'RESOURCE_NOT_FOUND' } } }, 404),
      check: (cache, rows) => {
        eq((cache[E1] || {}).a, TXT('Scopus E1.'), 'E1 cached from the Scopus batch');
        eq((cache[E5] || {}).a, TXT('Scopus E5.'), 'E5 (legacy PII DOI with parentheses, stale keyless miss) re-checked under the token and cached from Scopus');
        ok(cache[E8] && cache[E8].none === 1 && cache[E8].k === 2 && cache[E8].ttl === undefined, 'E8 (Scopus record WITHOUT text) is a tier-2 verdict');
        ok(!perDoiUrls().some(u => u.includes(E8)), 'E8 was not re-asked of the per-DOI leg (same Scopus record)');
        eq((cache[E2] || {}).a, TXT('Retrieval E2.'), 'E2 (not returned by Scopus) resolved by the per-DOI leg');
        ok(cache[E3] && cache[E3].none === 1 && cache[E3].k === 2 && cache[E3].t === today, 'E3 (200 but no abstract, after the leg had proved the token with E2) stamped a tier-2 miss');
        ok(cache[E4] === undefined && perDoiUrls().some(u => u.includes(`/content/abstract/doi/${E4}?`)), 'E4 (503 from the per-DOI leg) was asked and left uncached for the next run');
        ok(cache[E6] && cache[E6].k === 2 && cache[E6].t === today - 1, 'E6 (fresh tier-2 miss) untouched — not re-queried');
        ok(cache[E7] && cache[E7].k === 2 && cache[E7].t === today, 'E7 (fresh tier-1 miss) re-checked under the token and re-stamped tier 2');
        ok(cache[E9] && cache[E9].k === 2 && cache[E9].ttl === 7, 'E9 (404 on a current-year paper) stamped a 7-day miss — Scopus may index it next week');
        ok(cache[E10] && cache[E10].k === 2 && cache[E10].ttl === undefined, 'E10 (404 on a 2015 paper) stamped a plain tier-2 miss');
        ok(cache[O1] && cache[O1].none === 1 && cache[O1].k === undefined, 'O1 (OUP, no keyed leg) stamped a plain miss with no tier');
        ok(cache[S1] && cache[S1].none === 1 && cache[S1].k === undefined, 'S1 (Springer, no Springer key) stamped a plain miss with no tier');
        eq((cache[O2] || {}).a, 'We study markets in equilibrium and find that prices clear when traders share information about fundamentals.', 'O2 cached from the OpenAlex inverted index');
        const byDoi = Object.fromEntries(rows.map(r => [r.DOI.toLowerCase(), r]));
        eq(byDoi[E1].Abstract, TXT('Scopus E1.'), 'E1 abstract applied to the papers file');
        eq(byDoi[E2].Abstract, TXT('Retrieval E2.'), 'E2 abstract applied to the papers file');
        eq(byDoi[E5].Abstract, TXT('Scopus E5.'), 'E5 abstract applied to the papers file');
        eq(byDoi['10.1016/j.ejor.2019.01.099'].Abstract.length, 400, 'a row with a full abstract is untouched');
        eq(scopusUrls().length, 1, 'one Scopus query for the batch (nine Elsevier DOIs ≤ 25)');
        const u = new URL(scopusUrls()[0]);
        eq(u.searchParams.get('view'), 'COMPLETE', 'Scopus asked for the COMPLETE view');
        eq(u.searchParams.get('count'), '25', 'Scopus page size 25');
        ok(u.searchParams.get('query').includes(`DOI({${E5}})`), 'the PII DOI travels inside braces, parentheses and all');
        ok(!calls.some(u => u.includes(E6)), 'E6 never reached any API');
        ok(!perDoiUrls().some(u => u.includes(E1)) && !perDoiUrls().some(u => u.includes(E5)), 'DOIs Scopus served were not re-fetched per DOI');
        ok(perDoiUrls().some(u => u.includes(`/content/abstract/doi/${E2}?`)), 'the per-DOI URL carries the DOI with its slash literal');
        ok(!calls.some(u => u.includes('tok-xyz') || u.includes('key-abc')), 'neither the key nor the token ever appears in a URL');
        ok(seenHeaders.every(h => h['X-ELS-APIKey'] === 'key-abc' && h['X-ELS-Insttoken'] === 'tok-xyz'), 'every Elsevier request carried both headers');
        ok(seenHeaders.length >= 2, 'both Elsevier endpoints were exercised');
      },
    },
    // Scopus refuses the COMPLETE view (no subscription behind the token): the
    // per-DOI leg still does the work and the run reports why.
    'scopus-refused': {
      cache: {},
      scopus: () => json({ 'service-error': { status: { statusCode: 'AUTHORIZATION_ERROR', statusText: 'The requestor is not authorized to access the requested view or fields of the resource' } } }, 401),
      abstract: (doi) => doi === E1 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:description': TXT('Retrieval E1.') } } })
        : json({ 'abstracts-retrieval-response': { coredata: {} } }),
      check: (cache) => {
        eq(scopusUrls().length, 1, 'Scopus was tried once and then dropped for the run');
        eq((cache[E1] || {}).a, TXT('Retrieval E1.'), 'E1 still resolved by the per-DOI leg');
        ok(cache[E2] && cache[E2].k === 2, 'E2 stamped tier 2 (checked with key + token, after the leg found E1)');
        ok(cache[E3] && cache[E3].k === 2, 'E3 stamped tier 2');
      },
    },
    // Key without the token: Scopus is never called, misses are tier 1, a
    // fresh tier-1 miss is not re-queried, a fresh keyless miss is.
    'key-only': {
      cache: { [E6]: { none: 1, t: today - 1, k: 1 }, [E7]: { none: 1, t: today - 1 } },
      scopus: () => json({}, 500),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: { 'dc:title': 'metadata only' } } }),
      check: (cache) => {
        eq(scopusUrls().length, 0, 'Scopus is not called without the institutional token');
        ok(cache[E1] && cache[E1].k === 1, 'E1 stamped a tier-1 miss (key only)');
        ok(cache[E6] && cache[E6].t === today - 1, 'E6 (fresh tier-1 miss) not re-queried');
        ok(!calls.some(u => u.includes(E6)), 'E6 never reached the API');
        ok(cache[E7] && cache[E7].k === 1 && cache[E7].t === today, 'E7 (fresh keyless miss) re-checked under the key and re-stamped tier 1');
        ok(seenHeaders.every(h => h['X-ELS-APIKey'] === 'key-abc' && !('X-ELS-Insttoken' in h)), 'no Insttoken header is sent when none is configured');
      },
    },
    // Scopus rejects the exact DOI({…}) form: the chunk is retried once as
    // DOI("…") and the run continues on that form.
    'scopus-400-fallback': {
      cache: {},
      scopus: (dois, url) => url.includes(encodeURIComponent('DOI({')) ? json({ 'service-error': { status: { statusCode: 'INVALID_INPUT' } } }, 400)
        : json({ 'search-results': { entry: scopusEntries(dois, { [E1]: { text: TXT('Loose E1.') } }) } }),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: {} } }),
      check: (cache) => {
        eq(scopusUrls().length, 2, 'two Scopus calls: the rejected exact form, then the loose form');
        ok(scopusUrls()[1].includes(encodeURIComponent('DOI("')), 'the retry used the DOI("…") form');
        eq((cache[E1] || {}).a, TXT('Loose E1.'), 'E1 found on the fallback form');
      },
    },
    // The weekly Abstract Retrieval quota runs out mid-batch: the leg stops
    // and the DOIs it never reached are NOT written off.
    'quota-spent': {
      cache: {},
      scopus: () => json({}, 500),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: {} } }, 200,
        { 'X-RateLimit-Limit': '10000', 'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset': '1789000000' }),
      check: (cache) => {
        eq(scopusUrls().length, 0, 'FT50_ABS_SCOPUS=0 keeps the Scopus leg off');
        eq(perDoiUrls().length, 1, 'the per-DOI leg stopped after the answer that said the quota is spent');
        const stamped = [E1, E2, E3, E4, E5, E8, E9, E10].filter(d => cache[d]);
        eq(stamped.length, 1, 'only the one DOI actually answered is stamped; the rest stay uncached');
        ok(cache[O1] && cache[O1].none === 1, 'non-Elsevier DOIs are still stamped by the batched legs');
      },
    },
    // The token does not unlock text: every per-DOI answer is 200 without a
    // description. Nothing may be written off at tier 2, and the run must say so.
    'token-no-text': {
      rows: () => oldRows(30),
      cache: {},
      scopus: () => json(EMPTY),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: { 'dc:title': 'metadata only' } } }, 200, { 'X-RateLimit-Remaining': '9000' }),
      check: (cache) => {
        const misses = Object.values(cache).filter(v => v && v.none);
        eq(misses.length, 30, 'all thirty DOIs were answered and stamped');
        ok(misses.every(v => v.k === 1), 'every no-text answer of a run that retrieved nothing is stamped at TIER 1, never tier 2');
        eq(perDoiUrls().length, 30, 'the per-DOI leg kept going (it was not dropped)');
      },
    },
    // Scopus accepts the exact form but matches nothing for five chunks of
    // older papers: the run switches to the quoted form and carries on. The
    // per-DOI leg is out (quota), so the first batch's unreturned DOIs get
    // provisional 7-day stamps instead of being re-asked every run.
    'scopus-empty-streak': {
      rows: () => oldRows(150),
      cache: {},
      scopus: (dois, url) => url.includes(encodeURIComponent('DOI({')) ? json(EMPTY)
        : json({ 'search-results': { entry: scopusEntries(dois, Object.fromEntries(dois.map(d => [d, { text: TXT(`Loose ${d}.`) }]))) } }),
      abstract: () => json({ 'service-error': { status: { statusCode: 'QUOTA_EXCEEDED', statusText: 'Quota Exceeded' } } }, 429,
        { 'X-RateLimit-Limit': '10000', 'X-RateLimit-Remaining': '0' }),
      check: (cache) => {
        const exact = scopusUrls().filter(u => u.includes(encodeURIComponent('DOI({'))).length;
        const loose = scopusUrls().filter(u => u.includes(encodeURIComponent('DOI("'))).length;
        eq(exact, 5, 'five exact-form chunks matched nothing');
        eq(loose, 2, 'the fifth chunk was retried on the quoted form and the sixth ran on it');
        const hits = Object.entries(cache).filter(([k, v]) => k.startsWith('10.1016/') && v && v.a).length;
        eq(hits, 50, 'the two quoted-form chunks resolved their fifty DOIs');
        eq(perDoiUrls().length, 1, 'the per-DOI leg dropped on its first (quota) answer');
        const prov = Object.values(cache).filter(v => v && v.none && v.ttl === 7 && v.k === 2).length;
        eq(prov, 100, 'the hundred DOIs Scopus did not return while the per-DOI leg was out carry provisional 7-day tier-2 stamps');
      },
    },
    // The Springer leg (the Nature shard's ONLY keyed leg): a find is cached and
    // applied, an empty answer is a tier-1 miss, a 5xx leaves the DOI uncached.
    'springer': {
      rows: () => [
        { DOI: S1, Title: 'Paper S1', Authors: 'L. Author', Journal: 'MkLett', JKey: 'mlet', Year: '2024', Abstract: '' },
        { DOI: SPR2, Title: 'Paper SPR2', Authors: 'R. Author', Journal: 'MkLett', JKey: 'mlet', Year: '2024', Abstract: '' },
        { DOI: SPR3, Title: 'Paper SPR3', Authors: 'S. Author', Journal: 'MkLett', JKey: 'mlet', Year: '2024', Abstract: '' },
        { DOI: O1, Title: 'Paper O1', Authors: 'J. Author', Journal: 'QJE', JKey: 'qje', Year: String(NOW_YEAR), Abstract: '' },
      ],
      cache: {},
      scopus: () => json({}, 500), abstract: () => json({}, 500),
      springer: (doi) => doi === S1 ? json({ records: [{ abstract: `Abstract ${TXT('Springer S1.')}` }] })
        : doi === SPR2 ? json({ records: [] }) : json({ message: 'boom' }, 503),
      check: (cache, rows) => {
        eq(springerUrls().length, 3, 'three Springer calls, one per Springer DOI');
        eq(calls.filter(u => u.includes('api.elsevier.com')).length, 0, 'no Elsevier call without an Elsevier key');
        eq((cache[S1] || {}).a, TXT('Springer S1.'), 'S1 cached from the Springer Meta API ("Abstract " prefix stripped)');
        eq(rows.find(r => r.DOI === S1).Abstract, TXT('Springer S1.'), 'S1 applied to the papers file');
        ok(cache[SPR2] && cache[SPR2].none === 1 && cache[SPR2].k === 1 && cache[SPR2].t === today, 'SPR2 (no record) stamped a tier-1 miss');
        ok(cache[SPR3] === undefined, 'SPR3 (503) left uncached for the next run');
        ok(cache[O1] && cache[O1].none === 1 && cache[O1].k === undefined, 'O1 stamped a plain miss');
        ok(springerUrls().every(u => u.includes('api_key=spr-key')) && !calls.some(u => !u.includes('springernature') && u.includes('spr-key')), 'the Springer key travels only in Springer URLs (that API\'s own design)');
      },
    },
    // No Elsevier key at all — the live configuration of a shard until its
    // secrets are set: behaviour unchanged, plain tier-less misses, a young
    // tier-2 miss left exactly as it is.
    'no-key': {
      rows: () => [
        { DOI: E1, Title: 'Paper E1', Authors: 'A. Author', Journal: 'EJOR', JKey: 'ejor', Year: String(NOW_YEAR), Abstract: '' },
        { DOI: E6, Title: 'Paper E6', Authors: 'G. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2020', Abstract: '' },
        { DOI: O1, Title: 'Paper O1', Authors: 'J. Author', Journal: 'QJE', JKey: 'qje', Year: String(NOW_YEAR), Abstract: '' },
      ],
      cache: { [E6]: { none: 1, t: today - 1, k: 2 } },
      scopus: () => json({}, 500), abstract: () => json({}, 500),
      check: (cache) => {
        eq(calls.filter(u => u.includes('api.elsevier.com')).length, 0, 'no Elsevier call without a key');
        eq(JSON.stringify(cache[E1]), JSON.stringify({ none: 1, t: today }), 'E1 stamped a plain tier-less miss');
        eq(JSON.stringify(cache[O1]), JSON.stringify({ none: 1, t: today }), 'O1 stamped a plain tier-less miss');
        eq(JSON.stringify(cache[E6]), JSON.stringify({ none: 1, t: today - 1, k: 2 }), 'the young tier-2 miss is byte-identical');
        const oa = calls.find(u => u.startsWith('https://api.openalex.org/'));
        ok(oa && oa.includes(E1) && oa.includes(O1) && !oa.includes(E6), 'OpenAlex was asked only for the two eligible DOIs');
      },
    },
    // The push-retry replay: --apply-only --merge-cache=<ours> takes a
    // stronger-tier miss and a find, never a keyless miss over a tier-2 one,
    // applies the find to the row, and fetches nothing.
    'merge-apply': {
      cache: { [E1]: { none: 1, t: today - 3 }, [E2]: { none: 1, t: today - 1, k: 2 }, [E3]: { none: 1, t: today - 1 } },
      before: (dir) => writeFileSync(join(dir, 'ours.json'), JSON.stringify({
        [E1]: { none: 1, t: today, k: 2 }, [E2]: { none: 1, t: today }, [E3]: { a: TXT('Merged E3.') } })),
      argv: (dir) => ['--apply-only', `--merge-cache=${join(dir, 'ours.json')}`],
      scopus: () => json({}, 500), abstract: () => json({}, 500),
      check: (cache, rows) => {
        eq(calls.length, 0, 'apply-only fetches nothing');
        eq(JSON.stringify(cache[E1]), JSON.stringify({ none: 1, t: today, k: 2 }), 'a tier-2 miss replaces the keyless one');
        eq(JSON.stringify(cache[E2]), JSON.stringify({ none: 1, t: today - 1, k: 2 }), 'a keyless miss never replaces a tier-2 one');
        eq((cache[E3] || {}).a, TXT('Merged E3.'), 'the find was taken');
        eq(rows.find(r => r.DOI === E3).Abstract, TXT('Merged E3.'), 'and applied to the papers row');
      },
    },
    // The cache outgrows its per-part cap: it is written in PARTS (so a push
    // can never carry a file over GitHub's 100 MiB limit — three
    // lit-data-abs3-omecon runs did all their work and had every push
    // rejected at 102.41 MB), every part is read back, and a MULTI-PART merge
    // copy hands over the finds in its later parts too (the workflow's
    // push-retry replay copies the whole set, not just the first file).
    'chunked': {
      cache: Object.fromEntries(Array.from({ length: 40 }, (_, i) =>
        [`10.1016/j.ejor.2014.09.${String(i + 1).padStart(3, '0')}`, { a: TXT(`Bulk ${i + 1}.`) }])),
      before: (dir) => {
        writeFileSync(join(dir, 'ours.json'), JSON.stringify({ [E1]: { a: TXT('Merged from part 1.') } }));
        writeFileSync(join(dir, 'ours-2.json'), JSON.stringify({ [E3]: { a: TXT('Merged from part 2.') } }));
      },
      argv: (dir) => ['--apply-only', `--merge-cache=${join(dir, 'ours.json')}`],
      scopus: () => json({}, 500), abstract: () => json({}, 500),
      check: (cache, rows) => {
        ok(existsSync(join(DIR, '_api-abstracts-2.json')), 'the cache was split: a second part exists on disk');
        const part1 = JSON.parse(readFileSync(join(DIR, '_api-abstracts.json'), 'utf8'));
        ok(Object.keys(part1).length < 42, 'part 1 alone does not hold the whole cache');
        eq(Object.keys(cache).length, 42, 'every entry survives the split (40 seeded + 2 merged)');
        eq((cache[E1] || {}).a, TXT('Merged from part 1.'), "the merge copy's first part was taken");
        eq((cache[E3] || {}).a, TXT('Merged from part 2.'), "and so was its SECOND part");
        eq(rows.find(r => r.DOI === E3).Abstract, TXT('Merged from part 2.'), 'a find from a later merge part reaches the papers row');
        const bulk = '10.1016/j.ejor.2014.09.040';
        eq((cache[bulk] || {}).a, TXT('Bulk 40.'), 'a seeded entry that the split moved into a later part is still there');
      },
    },
    // The time budget runs out while the batched legs are still answering:
    // nothing keyed was tried, so nothing keyed is stamped.
    'budget-mid': {
      cache: {},
      setup: () => { openalexDelayMs = 2000; },
      scopus: () => json({}, 500), abstract: () => json({}, 500),
      check: (cache) => {
        eq(calls.filter(u => u.includes('api.elsevier.com')).length, 0, 'no Elsevier call once the budget is spent');
        ok([E1, E2, E3, E5, E8, E9, E10, S1].every(d => cache[d] === undefined), 'Elsevier and Springer DOIs the keyed legs never reached are not stamped');
        ok(cache[O1] && cache[O1].none === 1, 'the batched legs\' verdict on the OUP DOI still stands');
      },
    },
    // OpenAlex throttles: the run ends cleanly with nothing stamped.
    'openalex-429': {
      cache: {},
      setup: () => { openalexStatus = 429; },
      scopus: () => json({}, 500), abstract: () => json({}, 500),
      check: (cache) => {
        eq(calls.length, 1, 'exactly one call was made');
        eq(Object.keys(cache).length, 0, 'nothing was stamped');
      },
    },
    // A chrome-only cached "abstract" is healed to a TIER-LESS miss at load and
    // re-queried in the same token run.
    'heal-restamp': {
      cache: { [E1]: { a: CHROME } },
      scopus: (dois) => json({ 'search-results': { entry: scopusEntries(dois, { [E1]: { text: TXT('Healed E1.') } }) } }),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: {} } }),
      check: (cache) => {
        eq((cache[E1] || {}).a, TXT('Healed E1.'), 'the healed entry was re-opened under the token and resolved in the same run');
      },
    },
    // Scopus: a 5xx chunk falls through to the per-DOI leg uncached; a 200 that
    // says the quota is now spent still caches its finds, then drops the leg.
    'scopus-mixed': {
      rows: () => oldRows(50),
      cache: {},
      scopus: (() => { let n = 0; return (dois) => n++ === 0 ? json({ 'service-error': {} }, 503)
        : json({ 'search-results': { entry: scopusEntries(dois, Object.fromEntries(dois.map(d => [d, { text: TXT(`Scopus ${d}.`) }]))) } }, 200,
          { 'X-RateLimit-Limit': '20000', 'X-RateLimit-Remaining': '0' }); })(),
      abstract: () => json({ 'service-error': { status: { statusCode: 'RESOURCE_NOT_FOUND' } } }, 404, { 'X-RateLimit-Remaining': '9000' }),
      check: (cache, rows) => {
        eq(scopusUrls().length, 2, 'two Scopus calls: the 503 chunk and the one that spent the quota');
        const first = rows.slice(0, 25).map(r => r.DOI), second = rows.slice(25).map(r => r.DOI);
        ok(second.every(d => cache[d] && cache[d].a), 'the quota-spending answer\'s 25 finds were still cached');
        ok(first.every(d => cache[d] && cache[d].none === 1 && cache[d].k === 2 && cache[d].ttl === undefined), 'the 503 chunk\'s DOIs went to the per-DOI leg and were settled there');
        eq(perDoiUrls().length, 25, 'exactly the 503 chunk\'s DOIs were asked per DOI');
      },
    },
    // The per-DOI fetch itself throws, with the token quoted in the message
    // (undici's header validator does exactly that): the log must not carry it,
    // the leg drops for the run and the DOI stays uncached.
    'fetch-throws': {
      cache: {},
      scopus: () => json(EMPTY),
      abstract: () => { throw new TypeError('Headers.append: "tok-xyz" is an invalid header value.'); },
      check: (cache) => {
        eq(perDoiUrls().length, 1, 'the per-DOI leg stopped at the throw');
        ok(cache[E1] === undefined, 'the DOI whose fetch threw stays uncached');
        ok(cache[O1] && cache[O1].none === 1, 'the batched legs still stamped the non-Elsevier DOIs');
      },
    },
    // A 429 with quota left is the per-second throttle: wait, retry once, carry on.
    'throttle-429': {
      cache: {},
      scopus: () => json(EMPTY),
      abstract: (() => { let n = 0; return (doi) => (doi === E1 && n++ === 0)
        ? json({ 'service-error': { status: { statusCode: 'TOO_MANY_REQUESTS' } } }, 429, { 'X-RateLimit-Remaining': '9990' })
        : doi === E1 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:description': TXT('Retrieval E1.') } } }, 200, { 'X-RateLimit-Remaining': '9989' })
        : json({ 'service-error': {} }, 404, { 'X-RateLimit-Remaining': '9988' }); })(),
      check: (cache) => {
        eq(perDoiUrls().filter(u => u.includes(`/content/abstract/doi/${E1}?`)).length, 2, 'E1 was retried once after the throttle');
        eq((cache[E1] || {}).a, TXT('Retrieval E1.'), 'E1 found on the retry');
        ok(perDoiUrls().length > 2, 'the leg carried on with the other DOIs');
      },
    },
  }[name];
  if (!scen) { console.error(`unknown scenario ${name}`); process.exit(2); }
  if (scen.setup) scen.setup();
  globalThis.fetch = async (url, opts = {}) => {
    const u = String(url);
    calls.push(u);
    if (u.includes('api.elsevier.com')) seenHeaders.push({ ...(opts.headers || {}) });
    if (u.startsWith('https://api.openalex.org/works')) {
      if (openalexDelayMs) await new Promise(r => setTimeout(r, openalexDelayMs));
      if (openalexStatus !== 200) return json({ error: 'throttled' }, openalexStatus);
      return json({ results: [{ doi: `https://doi.org/${O2}`, abstract_inverted_index: {
        We: [0], study: [1], markets: [2], in: [3], equilibrium: [4], and: [5], find: [6], that: [7], prices: [8], clear: [9],
        when: [10], traders: [11], share: [12], information: [13], about: [14], 'fundamentals.': [15] } }] });
    }
    if (u.startsWith('https://api.semanticscholar.org/')) {
      const ids = JSON.parse(opts.body).ids;
      return json(ids.map(() => null));
    }
    if (u.startsWith('https://api.elsevier.com/content/search/scopus')) {
      const q = new URL(u).searchParams.get('query') || '';
      const dois = [...q.matchAll(/DOI\((?:\{([^}]+)\}|"([^"]+)")\)/g)].map(m => (m[1] || m[2]).toLowerCase());
      return scen.scopus(dois, u);
    }
    if (u.startsWith('https://api.elsevier.com/content/abstract/doi/')) {
      const doi = decodeURIComponent(u.slice('https://api.elsevier.com/content/abstract/doi/'.length).split('?')[0]);
      return scen.abstract(doi);
    }
    if (u.startsWith('https://api.springernature.com/')) {
      const q = new URL(u).searchParams.get('q') || '';
      const doi = q.replace(/^doi:"?/, '').replace(/"$/, '').toLowerCase();
      return scen.springer ? scen.springer(doi) : json({}, 500);
    }
    throw new Error(`unexpected fetch ${u}`);
  };
  const rows = scen.rows ? scen.rows() : baseRows();
  writeFileSync(join(DIR, 'papers-ejor.json'), JSON.stringify(rows));
  writeFileSync(join(DIR, '_api-abstracts.json'), JSON.stringify(scen.cache));
  if (scen.before) scen.before(DIR);
  if (scen.argv) process.argv.push(...scen.argv(DIR));
  console.log(`  [scenario ${name}]`);
  await main();
  // Read through every PART: the cache is chunked, and checking part 1 alone
  // would pass vacuously on whatever the split moved into part 2.
  const cache = readChunkedJsonSync(join(DIR, '_api-abstracts.json'), {});
  const outRows = JSON.parse(readFileSync(join(DIR, 'papers-ejor.json'), 'utf8'));
  scen.check(cache, outRows);
  console.log(fails ? `  scenario ${name}: FAILED (${fails})` : `  scenario ${name}: passed`);
  process.exit(fails ? 1 : 0);
}

console.log('invertedToText: OpenAlex abstract_inverted_index → text');
eq(invertedToText({ We: [0], study: [1], markets: [2, 4], in: [3] }),
  'We study markets in markets', 'positions reassembled in order, repeated words placed twice');
eq(invertedToText({}), '', 'empty index → empty string');
eq(invertedToText(null), '', 'null index → empty string');
eq(invertedToText({ a: 'junk' }), '', 'malformed positions ignored');

console.log('mergeAbsCache: abstract beats none, longer beats shorter, stronger miss beats weaker');
let c = { d1: { none: 1, t: 1 }, d2: { a: 'short' }, d3: { a: 'keep me intact' } };
const took = mergeAbsCache(c, { d1: { a: 'a full abstract text' }, d2: { a: 'a much longer abstract' }, d3: { none: 1, t: 9 }, d4: { a: 'brand new' } });
eq(took, 3, 'three entries taken (none→a, shorter→longer, new)');
eq(c.d1.a, 'a full abstract text', 'none upgraded to abstract');
eq(c.d2.a, 'a much longer abstract', 'shorter upgraded to longer');
eq(c.d3.a, 'keep me intact', 'a none-record never downgrades an abstract');
eq(c.d4.a, 'brand new', 'new entry taken');
{
  const m = { e1: { none: 1, t: 10 }, e2: { none: 1, t: 10, k: 2 }, e3: { none: 1, t: 10, k: 1 } };
  const n = mergeAbsCache(m, { e1: { none: 1, t: 12, k: 2 }, e2: { none: 1, t: 12 }, e3: { none: 1, t: 8, k: 1 } });
  eq(n, 1, 'exactly one miss replaced');
  eq(m.e1.k, 2, 'a token-checked miss replaces an older keyless write-off');
  eq(m.e2.t, 10, 'a keyless miss never replaces a token-checked one, even when newer');
  eq(m.e3.t, 10, 'same tier: an older stamp does not replace a newer one');
}

console.log('isNeedy: missing/stub abstracts only');
ok(isNeedy({ Abstract: '' }), 'missing abstract is needy');
ok(isNeedy({ Abstract: 'One-line teaser.' }), 'sub-300-char stub is needy');
ok(!isNeedy({ Abstract: 'x'.repeat(400) }), 'a real abstract is not needy');

console.log('elsevierAbstract: Abstract Retrieval JSON → text');
eq(elsevierAbstract({ 'abstracts-retrieval-response': { coredata: { 'dc:description': 'A plain abstract text.' } } }),
  'A plain abstract text.', 'plain-string dc:description');
eq(elsevierAbstract({ 'abstracts-retrieval-response': { coredata: { 'dc:description': { abstract: { 'ce:para': 'Nested para text.' } } } } }),
  'Nested para text.', 'nested ce:para object');
eq(elsevierAbstract({ 'abstracts-retrieval-response': { coredata: { 'dc:description': 'R&amp;D and CO&lt;sub&gt;2&lt;/sub&gt; costs.' } } }),
  'R&D and CO2 costs.', 'entities decoded + markup stripped via cleanText');
eq(elsevierAbstract({}), '', 'missing body → empty');
eq(elsevierAbstract(null), '', 'null → empty');

console.log('springerAbstract: Meta API v2 JSON → text');
eq(springerAbstract({ records: [{ abstract: 'Abstract We study queueing networks under load.' }] }),
  'We study queueing networks under load.', 'plain abstract, "Abstract " prefix stripped');
eq(springerAbstract({ records: [{ abstract: 'R&amp;D alliances and CO&lt;sub&gt;2&lt;/sub&gt; policy.' }] }),
  'R&D alliances and CO2 policy.', 'entities decoded + markup stripped via cleanText');
eq(springerAbstract({ records: [] }), '', 'no records → empty');
eq(springerAbstract({}), '', 'missing records → empty');
eq(springerAbstract(null), '', 'null → empty');

console.log('betterAbstract (the inlined shared upgrade rule)');
ok(betterAbstract('', 'y'.repeat(80)), 'empty ← candidate works via the inlined rule');


console.log('stripPageFurniture guard (feedback LIT-260727-XRQ8)');
eq(elsevierAbstract({ 'abstracts-retrieval-response': { coredata: { 'dc:description':
  'Previous articleNext article No AccessSome Paper TitleSome AuthorPDFPDF PLUS Add to favoritesDownload CitationTrack CitationsPermissionsReprints Share onFacebookXLinkedIn' } } }),
  '', 'a scraped page-chrome blob is rejected, never served as an abstract');
eq(springerAbstract({ records: [{ abstract: 'Journal Article Some Title Get access A. Author Search for other works by this author on: Oxford Academic Google Scholar' } ] }),
  '', 'an OUP-style page-header scrape is rejected');
eq(elsevierAbstract({ 'abstracts-retrieval-response': { coredata: { 'dc:description':
  'A real abstract that legitimately says firms fight their way back to top positions over time, with enough prose to look like an abstract.' } } }),
  'A real abstract that legitimately says firms fight their way back to top positions over time, with enough prose to look like an abstract.',
  '"back to top" inside real prose survives the guard');

console.log('shouldStampMiss: a keyed leg that never ran must not write off its DOIs');
{
  const ELS = /^10\.1016\//, SPR = /^10\.1007\//;
  const ejor = '10.1016/j.ejor.2026.05.001';
  const spr  = '10.1007/s11002-024-09999-9';
  const oup  = '10.1093/qje/qjaa001';
  const base = { elsPrefix: ELS, sprPrefix: SPR };

  // The real incident: ELSEVIER_API_KEY set, Elsevier refused it (401/403/429),
  // the leg dropped on its first call, so no EJOR DOI was ever queried. Those
  // must stay uncached and be retried next run, not written off for 45 days.
  ok(!shouldStampMiss(ejor, { ...base, elsKey: 'k', sprKey: '', keyedTried: new Set() }),
    'keyed Elsevier DOI the leg never reached is NOT stamped as a miss');
  ok(shouldStampMiss(ejor, { ...base, elsKey: 'k', sprKey: '', keyedTried: new Set([ejor]) }),
    'keyed Elsevier DOI the leg DID try is stamped (a genuine no-abstract)');
  // With no key configured there is no keyed leg to wait for, so the batched
  // OpenAlex/S2 legs' verdict stands — the pre-existing behaviour, unchanged.
  ok(shouldStampMiss(ejor, { ...base, elsKey: '', sprKey: '', keyedTried: new Set() }),
    'unkeyed run still stamps Elsevier DOIs (behaviour unchanged without a key)');
  ok(!shouldStampMiss(spr, { ...base, elsKey: '', sprKey: 'k', keyedTried: new Set() }),
    'the same rule protects Springer DOIs when only that key is set');
  ok(shouldStampMiss(spr, { ...base, elsKey: 'k', sprKey: '', keyedTried: new Set() }),
    'a Springer DOI is not protected by the Elsevier key');
  // A DOI no keyed leg owns is unaffected either way.
  ok(shouldStampMiss(oup, { ...base, elsKey: 'k', sprKey: 'k', keyedTried: new Set() }),
    'a non-Elsevier, non-Springer DOI is stamped normally');
}

console.log('credentialTier / missIsFresh / missStamp: a stronger credential re-opens weaker misses');
{
  const ELS = ELS_PREFIX, SPR = SPR_PREFIX; // the module's own prefixes, not local copies
  ok(ELS.test('10.1016/j.ejor.2026.05.001') && !ELS.test('10.1093/qje/qjaa001'), 'ELS_PREFIX owns 10.1016');
  ok(SPR.test('10.1007/s11002-024-09999-9') && SPR.test('10.1057/jors.2015.1') && !SPR.test('10.1016/j.ejor.2026.05.001'), 'SPR_PREFIX owns 10.1007/10.1057');
  ok(!SPR.test('10.1038/s41586-024-00001-1'), 'SPR_PREFIX leaves 10.1038 to the Nature shard');
  const ejor = '10.1016/j.ejor.2026.05.001', spr = '10.1007/s11002-024-09999-9', oup = '10.1093/qje/qjaa001';
  const cred = (elsKey, elsInsttoken, sprKey) => ({ elsKey, elsInsttoken, sprKey, elsPrefix: ELS, sprPrefix: SPR });
  eq(credentialTier(ejor, cred('', '', '')), 0, 'Elsevier DOI, no key → tier 0');
  eq(credentialTier(ejor, cred('k', '', '')), 1, 'Elsevier DOI, key only → tier 1');
  eq(credentialTier(ejor, cred('k', 't', '')), 2, 'Elsevier DOI, key + institutional token → tier 2');
  eq(credentialTier(ejor, cred('', 't', '')), 0, 'a token without a key is nothing (tier 0)');
  eq(credentialTier(spr, cred('k', 't', '')), 0, 'Springer DOI is not raised by the Elsevier credentials');
  eq(credentialTier(spr, cred('', '', 's')), 1, 'Springer DOI, Springer key → tier 1');
  eq(credentialTier(oup, cred('k', 't', 's')), 0, 'other publishers are always tier 0');
  const today = 1000;
  ok(!missIsFresh(undefined, { today, ttlDays: 45, tier: 0 }), 'no record → not fresh');
  ok(!missIsFresh({ a: 'x' }, { today, ttlDays: 45, tier: 0 }), 'a hit is not a miss');
  ok(missIsFresh({ none: 1, t: 990 }, { today, ttlDays: 45, tier: 0 }), 'young keyless miss binds an unkeyed run (the pre-existing TTL rule)');
  ok(!missIsFresh({ none: 1, t: 900 }, { today, ttlDays: 45, tier: 0 }), 'an expired miss is retried');
  ok(!missIsFresh({ none: 1, t: 990 }, { today, ttlDays: 45, tier: 2 }), 'a young KEYLESS miss is re-opened by a token run — the ~39k EJOR write-offs');
  ok(!missIsFresh({ none: 1, t: 990, k: 1 }, { today, ttlDays: 45, tier: 2 }), 'a young key-only miss (200 without text off-campus) is re-opened by a token run');
  ok(missIsFresh({ none: 1, t: 990, k: 2 }, { today, ttlDays: 45, tier: 2 }), 'a young token-checked miss binds a token run — no six-hourly thrash');
  ok(missIsFresh({ none: 1, t: 990, k: 2 }, { today, ttlDays: 45, tier: 1 }), 'a token-checked miss also binds a weaker (key-only) run');
  ok(!missIsFresh({ none: 1, t: 900, k: 2 }, { today, ttlDays: 45, tier: 2 }), 'a token-checked miss still expires after the TTL');
  ok(missIsFresh({ none: 1, t: 995, k: 2, ttl: 7 }, { today, ttlDays: 45, tier: 2 }), 'a 7-day stamp binds inside its own window');
  ok(!missIsFresh({ none: 1, t: 990, k: 2, ttl: 7 }, { today, ttlDays: 45, tier: 2 }), 'a 7-day stamp expires after 7 days, not 45');
  eq(JSON.stringify(missStamp(1000, 0)), '{"none":1,"t":1000}', 'tier 0 stamps no k (lean file)');
  eq(JSON.stringify(missStamp(1000, 2)), '{"none":1,"t":1000,"k":2}', 'tier 2 stamps k:2');
  eq(JSON.stringify(missStamp(1000, 2, 7)), '{"none":1,"t":1000,"k":2,"ttl":7}', 'a short TTL is stamped on the record');
  eq(JSON.stringify(missStamp(1000, 2, 0)), '{"none":1,"t":1000,"k":2}', 'ttl 0/undefined stamps nothing');
}

console.log('elsAnswerIsDefinitive: only 200 and 404 are verdicts');
ok(elsAnswerIsDefinitive(200), '200 (with or without text) is a verdict');
ok(elsAnswerIsDefinitive(404), '404 is a verdict');
ok(!elsAnswerIsDefinitive(400), '400 (INVALID_INPUT, about the request) is not a verdict on the paper');
ok(!elsAnswerIsDefinitive(401) && !elsAnswerIsDefinitive(403) && !elsAnswerIsDefinitive(429), 'the drop-the-leg trio are not verdicts');
ok(!elsAnswerIsDefinitive(500) && !elsAnswerIsDefinitive(503), 'a 5xx leaves the DOI for the next run');

console.log('elsErrorText / isRecentYear');
eq(elsErrorText({ 'service-error': { status: { statusCode: 'AUTHORIZATION_ERROR', statusText: 'The requestor is not authorized' } } }),
  'AUTHORIZATION_ERROR: The requestor is not authorized', 'code and text from the envelope');
eq(elsErrorText({ 'service-error': { status: { statusCode: 'QUOTA_EXCEEDED' } } }), 'QUOTA_EXCEEDED', 'code alone');
eq(elsErrorText({ 'abstracts-retrieval-response': {} }), '', 'no envelope → empty');
eq(elsErrorText(null), '', 'null → empty');
ok(isRecentYear('2026', 2026) && isRecentYear(2025, 2026), 'current and previous year are recent');
ok(!isRecentYear('2024', 2026) && !isRecentYear('', 2026) && !isRecentYear(undefined, 2026), 'older, blank or missing years are not');

console.log('headerSafeValue: a credential must be a header-safe value');
eq(headerSafeValue('  abc123-XYZ  '), 'abc123-XYZ', 'trimmed');
eq(headerSafeValue('abc\ndef'), '', 'an embedded line break is refused (undici would quote the value in its error)');
eq(headerSafeValue('abc def'), '', 'an inner space is refused');
eq(headerSafeValue('abc\u0000'), '', 'a control character is refused');
eq(headerSafeValue(''), '', 'empty stays empty');
eq(headerSafeValue(undefined), '', 'unset stays empty');

console.log('scopusDoiQuery: DOI({…}) OR …, with a quoted fallback form');
eq(scopusDoiQuery(['10.1016/j.ejor.2015.05.082', '10.1016/s0377-2217(99)00123-4']),
  'DOI({10.1016/j.ejor.2015.05.082}) OR DOI({10.1016/s0377-2217(99)00123-4})', 'exact form: braces keep the PII parentheses literal');
eq(scopusDoiQuery(['10.1016/j.ejor.2015.05.082'], 'loose'), 'DOI("10.1016/j.ejor.2015.05.082")', 'loose form: double quotes');
eq(scopusDoiQuery([]), '', 'empty chunk → empty query');

console.log('scopusAbstracts: Scopus Search JSON → Map(doi → text)');
{
  const m = scopusAbstracts({ 'search-results': { 'opensearch:totalResults': '3', entry: [
    { '@_fa': 'true', 'prism:doi': '10.1016/J.EJOR.2015.05.082', 'dc:description': 'A real abstract about routing. © 2015 Elsevier B.V.' },
    { '@_fa': 'true', 'prism:doi': 'https://doi.org/10.1016/j.ejor.2015.05.083', 'dc:title': 'Editorial' },
    { '@_fa': 'true', 'prism:doi': '10.1016/j.ejor.2015.05.084', 'dc:description': 'R&amp;D spillovers and CO&lt;sub&gt;2&lt;/sub&gt;.' },
    { '@_fa': 'true', 'dc:description': 'no doi on this one' },
    { '@_fa': 'true', 'prism:doi': '10.1016/j.ejor.2015.05.082', 'dc:description': 'shorter dup' },
  ] } });
  eq(m.get('10.1016/j.ejor.2015.05.082'), 'A real abstract about routing. © 2015 Elsevier B.V.', 'DOI lower-cased; longer text kept over a duplicate');
  eq(m.get('10.1016/j.ejor.2015.05.083'), '', 'a record without dc:description maps to empty text (matched, no text)');
  eq(m.get('10.1016/j.ejor.2015.05.084'), 'R&D spillovers and CO2.', 'doi.org prefix stripped; entities decoded');
  eq(m.size, 3, 'an entry without a DOI is skipped');
  eq(scopusAbstracts({ 'search-results': { 'opensearch:totalResults': '0', entry: [{ '@_fa': 'true', error: 'Result set was empty' }] } }).size, 0, 'the empty-result-set error entry is skipped');
  eq(scopusAbstracts({}).size, 0, 'missing body → empty map');
  eq(scopusAbstracts(null).size, 0, 'null → empty map');
  eq(scopusAbstracts({ 'search-results': { entry: [{ 'prism:doi': '10.1016/x', 'dc:description': '• A new measure • We provide • Results show' }] } }).get('10.1016/x'), '', 'a highlights-only description is dropped like every other leg');
}

console.log('readRateLimit: Elsevier X-RateLimit headers');
{
  const h = new Headers({ 'X-RateLimit-Limit': '20000', 'X-RateLimit-Remaining': '19990', 'X-RateLimit-Reset': '1789000000' });
  const q = readRateLimit(h);
  ok(q && q.limit === 20000 && q.remaining === 19990 && q.reset === 1789000000, 'all three parsed as numbers');
  eq(readRateLimit(new Headers({ 'content-type': 'application/json' })), null, 'no headers → null');
  const p = readRateLimit(new Headers({ 'X-RateLimit-Remaining': '0' }));
  ok(p && p.remaining === 0 && p.limit === null, 'a lone Remaining: 0 is read (the spent-quota signal)');
  eq(readRateLimit(null), null, 'null headers → null');
}

console.log('source pin: the institutional token and key travel only as request headers');
{
  const src = readFileSync(join(HERE, 'abstracts-ci.mjs'), 'utf8');
  ok(!/\$\{ELS_(INSTTOKEN|KEY)\b/.test(src), 'neither credential is ever interpolated into a URL or string');
  ok(!/\+\s*ELS_(INSTTOKEN|KEY)\b/.test(src) && !/\bELS_(INSTTOKEN|KEY)\s*\+/.test(src), 'neither credential is ever concatenated into a string');
  // A log line may ASK whether a credential is set (a `cred ?` ternary test or
  // a `!cred` / `!!cred` negation) and nothing more — never carry its value.
  const stripBoolUse = (l) => l.replace(/!!?ELS_(INSTTOKEN|KEY)\b/g, '').replace(/\bELS_(INSTTOKEN|KEY)\b(?=\s*\?)/g, '');
  ok(!src.split('\n').some(l => /console\.(log|warn|error)/.test(l) && /\bELS_(INSTTOKEN|KEY)\b/.test(stripBoolUse(l))), 'no log line names a credential value (a yes/no test of it is fine)');
  ok(!src.split('\n').some(l => /console\.(log|warn|error)/.test(l) && /\b(elsHeaders|CRED|elsInsttoken)\b/.test(l)), 'no log line dumps the headers object or the credential set');
  ok(!/console\.(log|warn|error)[^\n]*\be\.message\b/.test(src), 'no catch prints an error message verbatim (undici quotes a bad header VALUE in its message)');
  ok(/'X-ELS-Insttoken':\s*ELS_INSTTOKEN/.test(src) && /'X-ELS-APIKey':\s*ELS_KEY/.test(src), 'the token is the X-ELS-Insttoken header and the key the X-ELS-APIKey header');
  ok(!/api\.elsevier\.com[^'`]*(apiKey|insttoken)=/i.test(src), 'no Elsevier URL carries apiKey= or insttoken= query parameters');
  ok((src.match(/https:\/\/api\.elsevier\.com/g) || []).length >= 2 && !/http:\/\/api\.elsevier\.com/.test(src), 'every Elsevier endpoint is https');
}

// ── Every OTHER reader of this cache reads it through all its PARTS ─────────
// The cache is chunked, so a sibling that JSON.parses the first file alone
// silently drops every abstract the split moved into a later part — and the
// symptom (a paper losing an abstract it had) looks nothing like the cause.
// Pinned by source, because the daily build's read has no offline harness.
console.log('the cache is read through every part wherever it is read');
{
  const pin = (label, file, marker, wanted) => {
    if (!existsSync(file)) { ok(false, `${label}: found on disk`); return; }
    const src = readFileSync(file, 'utf8');
    const at = src.indexOf(marker);
    if (at < 0) { ok(false, `${label}: the marker "${marker}" is still there`); return; }
    // A slice taken on a marker that moved passes every negative check by
    // vacuity, so the slice's own size is asserted first.
    const slice = src.slice(at, at + 2500);
    ok(slice.length > 500, `${label}: the slice really is the block (${slice.length} chars)`);
    ok(wanted.test(slice), `${label}: reads the cache through every part`);
  };
  pin('the daily build (applyAbstractCaches)', join(HERE, 'build-data.mjs'),
    'async function applyAbstractCaches', /readChunkedJson\(/);
  const cleaner = [join(HERE, 'clean-junk-abstracts.mjs'), join(HERE, '..', '_scraper', 'clean-junk-abstracts.mjs')]
    .find((f) => existsSync(f));
  ok(!!cleaner, 'clean-junk-abstracts.mjs found (beside this test or in the shared scraper dir)');
  if (cleaner) pin('clean-junk-abstracts', cleaner, "const apiCachePath",
    /readChunkedJsonSync\([\s\S]*writeChunkedJson\(/);
}

// ── Whole-run scenarios (parent side) ───────────────────────────────────────
console.log('whole-run scenarios against a stubbed fetch (child processes)');
const EXPECT_STDOUT = {
  'token': /1 transient \(left for next run\)/,
  'token-no-text': /::warning::The institutional token is not unlocking abstract text: 30 Elsevier answers/,
  'scopus-empty-streak': /retrying this chunk as DOI\("…"\)/,
  'scopus-refused': /::warning::Scopus Search refused the COMPLETE view \(HTTP 401 \(AUTHORIZATION_ERROR/,
  'scopus-mixed': /Scopus leg dropped for this run \(weekly quota spent/,
  // The DOIs the spent quota never reached are not written off — and the run says how many it left behind.
  'quota-spent': /::notice::\d+ Elsevier DOIs were left uncached this run/,
  'openalex-429': /OpenAlex throttled \(HTTP 429\) — ending the run cleanly/,
  'heal-restamp': /healed 1 furniture\/highlights-contaminated cached abstracts/,
  // The run says where the cache went when it needed more than one part.
  'chunked': /merge-cache: took 2 entries from/,
};
const SCENARIOS = ['token', 'scopus-refused', 'key-only', 'scopus-400-fallback', 'quota-spent', 'token-no-text', 'scopus-empty-streak',
  'throttle-429', 'fetch-throws', 'springer', 'no-key', 'merge-apply', 'budget-mid', 'openalex-429', 'heal-restamp', 'scopus-mixed',
  'chunked'];
for (const name of SCENARIOS) {
  const dir = mkdtempSync(join(tmpdir(), `lit-abs-${name}-`));
  const env = { ...process.env, ABS_SCENARIO: name, FT50_DATA_DIR: dir,
    FT50_ABS_PACE_MS: '150', FT50_ABS_ELS_PACE_MS: '250', FT50_ABS_SCOPUS_PACE_MS: '250', FT50_ABS_SPR_PACE_MS: '250' };
  // A clean credential slate per scenario: only what the scenario sets.
  for (const k of ['ELSEVIER_API_KEY', 'ELSEVIER_INST_TOKEN', 'SPRINGER_API_KEY', 'S2_API_KEY', 'FT50_ABS_SCOPUS', 'FT50_ABS_S2',
    'FT50_ABS_SCOPUS_FORM', 'FT50_ABS_BUDGET_MS', 'FT50_ABS_MISS_TTL_DAYS', 'FT50_ABS_CHUNK_BYTES']) delete env[k];
  Object.assign(env, {
    'token': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'scopus-refused': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'key-only': { ELSEVIER_API_KEY: 'key-abc' },
    'scopus-400-fallback': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'quota-spent': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz', FT50_ABS_SCOPUS: '0' },
    'token-no-text': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'scopus-empty-streak': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'throttle-429': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'fetch-throws': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'springer': { SPRINGER_API_KEY: 'spr-key' },
    'no-key': {},
    'merge-apply': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'budget-mid': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz', SPRINGER_API_KEY: 'spr-key', FT50_ABS_BUDGET_MS: '1500' },
    'openalex-429': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'heal-restamp': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'scopus-mixed': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    // 4 KiB: enough that 40 bulky entries need several parts, small enough
    // that the test does not write tens of megabytes to prove it.
    'chunked': { FT50_ABS_CHUNK_BYTES: '4096' },
  }[name]);
  const r = spawnSync(process.execPath, [SELF], { env, encoding: 'utf8', timeout: 120000 });
  const lines = `${r.stdout || ''}${r.stderr || ''}`.split('\n').filter(l => /^\s+[✓✗]|scenario/.test(l));
  for (const l of lines) console.log(`  ${l.trim()}`);
  if (r.status !== 0) {
    fails++;
    console.error(`  ✗ scenario ${name} exited ${r.status}`);
    if (!lines.length) console.error((r.stdout || '') + (r.stderr || ''));
  }
  if (EXPECT_STDOUT[name]) ok(EXPECT_STDOUT[name].test(r.stdout || ''), `scenario ${name}: the run log says so (${EXPECT_STDOUT[name]})`);
  // The runtime pin that the source pin cannot give: the whole run's output —
  // every log line, every error path, object dumps included — never carries a
  // credential value. (The child asserts the URLs; this covers the log.)
  const out = (r.stdout || '') + (r.stderr || '');
  ok(!out.includes('tok-xyz') && !out.includes('key-abc') && !out.includes('spr-key'), `scenario ${name}: the run's own log never carries a credential value`);
  rmSync(dir, { recursive: true, force: true });
}

console.log(fails ? `\nFAILED (${fails})` : '\nAll abstract-backfill checks passed.');
process.exit(fails ? 1 : 0);
