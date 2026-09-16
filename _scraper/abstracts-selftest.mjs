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
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { invertedToText, mergeAbsCache, isNeedy, elsevierAbstract, springerAbstract, shouldStampMiss,
  credentialTier, missIsFresh, missStamp, elsAnswerIsDefinitive, scopusDoiQuery, scopusAbstracts,
  readRateLimit, main, betterAbstract } from './abstracts-ci.mjs';

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
  const E1 = '10.1016/j.ejor.2026.01.001', E2 = '10.1016/j.ejor.2026.01.002', E3 = '10.1016/j.ejor.2026.01.003';
  const E4 = '10.1016/j.ejor.2026.01.004', E5 = '10.1016/s0377-2217(99)00123-4', E6 = '10.1016/j.ejor.2020.01.006';
  const E7 = '10.1016/j.ejor.2021.01.007';
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
  let scopusCalls = 0, elsCalls = 0;
  const scen = {
    // The full token run: Scopus serves the bulk, the per-DOI leg the rest.
    'token': {
      env: { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
      cache: { [E5]: { none: 1, t: today - 1 }, [E6]: { none: 1, t: today - 1, k: 2 }, [E7]: { none: 1, t: today - 1, k: 1 } },
      scopus: (dois) => json({ 'search-results': { 'opensearch:totalResults': '3',
        entry: scopusEntries(dois, { [E1]: { text: TXT('Scopus E1.') }, [E2]: {}, [E5]: { text: TXT('Scopus E5.'), doiAs: '10.1016/S0377-2217(99)00123-4' } }) } },
        200, { 'X-RateLimit-Limit': '20000', 'X-RateLimit-Remaining': '19990', 'X-RateLimit-Reset': '1789000000' }),
      abstract: (doi) => doi === E2 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:description': TXT('Retrieval E2.') } } }, 200, { 'X-RateLimit-Remaining': '9990' })
        : doi === E3 || doi === E7 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:title': 'no description' } } }, 200, { 'X-RateLimit-Remaining': '9989' })
        : doi === E4 ? json({ 'service-error': { status: { statusCode: 'GENERIC_ERROR' } } }, 503)
        : json({ 'service-error': {} }, 404),
      check: (cache, rows) => {
        eq((cache[E1] || {}).a, TXT('Scopus E1.'), 'E1 cached from the Scopus batch');
        eq((cache[E5] || {}).a, TXT('Scopus E5.'), 'E5 (legacy PII DOI with parentheses, stale keyless miss) re-checked under the token and cached from Scopus');
        eq((cache[E2] || {}).a, TXT('Retrieval E2.'), 'E2 (Scopus record without text) resolved by the per-DOI leg');
        ok(cache[E3] && cache[E3].none === 1 && cache[E3].k === 2 && cache[E3].t === today, 'E3 (200 but no abstract under the token) stamped a tier-2 miss');
        ok(cache[E4] === undefined, 'E4 (503 from the per-DOI leg) left uncached for the next run');
        ok(cache[E6] && cache[E6].k === 2 && cache[E6].t === today - 1, 'E6 (fresh tier-2 miss) untouched — not re-queried');
        ok(cache[E7] && cache[E7].k === 2 && cache[E7].t === today, 'E7 (fresh tier-1 miss) re-checked under the token and re-stamped tier 2');
        ok(cache[O1] && cache[O1].none === 1 && cache[O1].k === undefined, 'O1 (OUP, no keyed leg) stamped a plain miss with no tier');
        ok(cache[S1] && cache[S1].none === 1 && cache[S1].k === undefined, 'S1 (Springer, no Springer key) stamped a plain miss with no tier');
        eq((cache[O2] || {}).a, 'We study markets in equilibrium and find that prices clear when traders share information about fundamentals.', 'O2 cached from the OpenAlex inverted index');
        const byDoi = Object.fromEntries(rows.map(r => [r.DOI.toLowerCase(), r]));
        eq(byDoi[E1].Abstract, TXT('Scopus E1.'), 'E1 abstract applied to the papers file');
        eq(byDoi[E2].Abstract, TXT('Retrieval E2.'), 'E2 abstract applied to the papers file');
        eq(byDoi[E5].Abstract, TXT('Scopus E5.'), 'E5 abstract applied to the papers file');
        eq(byDoi['10.1016/j.ejor.2019.01.009'].Abstract.length, 400, 'a row with a full abstract is untouched');
        const scopusUrls = calls.filter(u => u.includes('/content/search/scopus'));
        eq(scopusUrls.length, 1, 'one Scopus query for the batch (six Elsevier DOIs ≤ 25)');
        const u = new URL(scopusUrls[0]);
        eq(u.searchParams.get('view'), 'COMPLETE', 'Scopus asked for the COMPLETE view');
        eq(u.searchParams.get('count'), '25', 'Scopus page size 25');
        ok(u.searchParams.get('query').includes(`DOI({${E5}})`), 'the PII DOI travels inside braces, parentheses and all');
        ok(!calls.some(u => u.includes(E6)), 'E6 never reached any API');
        const perDoi = calls.filter(u => u.includes('/content/abstract/doi/'));
        ok(!perDoi.some(u => u.includes(encodeURIComponent(E1))) && !perDoi.some(u => u.includes(encodeURIComponent(E5))), 'DOIs Scopus served were not re-fetched per DOI');
        ok(perDoi.some(u => u.includes(encodeURIComponent(E2))), 'a DOI Scopus returned without text went to the per-DOI leg');
        ok(!calls.some(u => u.includes('tok-xyz') || u.includes('key-abc')), 'neither the key nor the token ever appears in a URL');
        ok(seenHeaders.every(h => h['X-ELS-APIKey'] === 'key-abc' && h['X-ELS-Insttoken'] === 'tok-xyz'), 'every Elsevier request carried both headers');
        ok(seenHeaders.length >= 2, 'both Elsevier endpoints were exercised');
      },
    },
    // Scopus refuses the COMPLETE view (no subscription behind the token): the
    // per-DOI leg still does the work and the run reports why.
    'scopus-refused': {
      env: { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
      cache: {},
      scopus: () => json({ 'service-error': { status: { statusCode: 'AUTHORIZATION_ERROR' } } }, 401),
      abstract: (doi) => doi === E1 ? json({ 'abstracts-retrieval-response': { coredata: { 'dc:description': TXT('Retrieval E1.') } } })
        : json({ 'abstracts-retrieval-response': { coredata: {} } }),
      check: (cache) => {
        eq(calls.filter(u => u.includes('/content/search/scopus')).length, 1, 'Scopus was tried once and then dropped for the run');
        eq((cache[E1] || {}).a, TXT('Retrieval E1.'), 'E1 still resolved by the per-DOI leg');
        ok(cache[E2] && cache[E2].k === 2, 'E2 stamped tier 2 (checked with key + token)');
        ok(cache[E3] && cache[E3].k === 2, 'E3 stamped tier 2');
      },
    },
    // Key without the token: Scopus is never called, misses are tier 1, a
    // fresh tier-1 miss is not re-queried, a fresh keyless miss is.
    'key-only': {
      env: { ELSEVIER_API_KEY: 'key-abc' },
      cache: { [E6]: { none: 1, t: today - 1, k: 1 }, [E7]: { none: 1, t: today - 1 } },
      scopus: () => json({}, 500),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: { 'dc:title': 'metadata only' } } }),
      check: (cache) => {
        eq(calls.filter(u => u.includes('/content/search/scopus')).length, 0, 'Scopus is not called without the institutional token');
        ok(cache[E1] && cache[E1].k === 1, 'E1 stamped a tier-1 miss (key only)');
        ok(cache[E6] && cache[E6].t === today - 1, 'E6 (fresh tier-1 miss) not re-queried');
        ok(!calls.some(u => u.includes(encodeURIComponent(E6))), 'E6 never reached the API');
        ok(cache[E7] && cache[E7].k === 1 && cache[E7].t === today, 'E7 (fresh keyless miss) re-checked under the key and re-stamped tier 1');
        ok(seenHeaders.every(h => h['X-ELS-APIKey'] === 'key-abc' && !('X-ELS-Insttoken' in h)), 'no Insttoken header is sent when none is configured');
      },
    },
    // Scopus rejects the exact DOI({…}) form: the chunk is retried once as
    // DOI("…") and the run continues on that form.
    'scopus-400-fallback': {
      env: { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
      cache: {},
      scopus: (dois, url) => url.includes(encodeURIComponent('DOI({')) ? json({ 'service-error': { status: { statusCode: 'INVALID_INPUT' } } }, 400)
        : json({ 'search-results': { entry: scopusEntries(dois, { [E1]: { text: TXT('Loose E1.') } }) } }),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: {} } }),
      check: (cache) => {
        const scopusUrls = calls.filter(u => u.includes('/content/search/scopus'));
        eq(scopusUrls.length, 2, 'two Scopus calls: the rejected exact form, then the loose form');
        ok(scopusUrls[1].includes(encodeURIComponent('DOI("')), 'the retry used the DOI("…") form');
        eq((cache[E1] || {}).a, TXT('Loose E1.'), 'E1 found on the fallback form');
      },
    },
    // The weekly Abstract Retrieval quota runs out mid-batch: the leg stops
    // and the DOIs it never reached are NOT written off.
    'quota-spent': {
      env: { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz', FT50_ABS_SCOPUS: '0' },
      cache: {},
      scopus: () => json({}, 500),
      abstract: () => json({ 'abstracts-retrieval-response': { coredata: {} } }, 200,
        { 'X-RateLimit-Limit': '10000', 'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset': '1789000000' }),
      check: (cache) => {
        eq(calls.filter(u => u.includes('/content/search/scopus')).length, 0, 'FT50_ABS_SCOPUS=0 keeps the Scopus leg off');
        const perDoi = calls.filter(u => u.includes('/content/abstract/doi/'));
        eq(perDoi.length, 1, 'the per-DOI leg stopped after the answer that said the quota is spent');
        const stamped = [E1, E2, E3, E4, E5].filter(d => cache[d]);
        eq(stamped.length, 1, 'only the one DOI actually answered is stamped; the rest stay uncached');
        ok(cache[O1] && cache[O1].none === 1, 'non-Elsevier DOIs are still stamped by the batched legs');
      },
    },
  }[name];
  if (!scen) { console.error(`unknown scenario ${name}`); process.exit(2); }
  globalThis.fetch = async (url, opts = {}) => {
    const u = String(url);
    calls.push(u);
    if (u.includes('api.elsevier.com')) seenHeaders.push({ ...(opts.headers || {}) });
    if (u.startsWith('https://api.openalex.org/works')) {
      return json({ results: [{ doi: `https://doi.org/${O2}`, abstract_inverted_index: {
        We: [0], study: [1], markets: [2], in: [3], equilibrium: [4], and: [5], find: [6], that: [7], prices: [8], clear: [9],
        when: [10], traders: [11], share: [12], information: [13], about: [14], 'fundamentals.': [15] } }] });
    }
    if (u.startsWith('https://api.semanticscholar.org/')) {
      const ids = JSON.parse(opts.body).ids;
      return json(ids.map(() => null));
    }
    if (u.startsWith('https://api.elsevier.com/content/search/scopus')) {
      scopusCalls++;
      const q = new URL(u).searchParams.get('query') || '';
      const dois = [...q.matchAll(/DOI\((?:\{([^}]+)\}|"([^"]+)")\)/g)].map(m => (m[1] || m[2]).toLowerCase());
      return scen.scopus(dois, u);
    }
    if (u.startsWith('https://api.elsevier.com/content/abstract/doi/')) {
      elsCalls++;
      const doi = decodeURIComponent(u.slice('https://api.elsevier.com/content/abstract/doi/'.length).split('?')[0]);
      return scen.abstract(doi);
    }
    throw new Error(`unexpected fetch ${u}`);
  };
  const rows = [
    { DOI: E1, Title: 'Paper E1', Authors: 'A. Author, B. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2026', Abstract: '' },
    { DOI: E2, Title: 'Paper E2', Authors: 'C. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2026', Abstract: 'Teaser only.' },
    { DOI: E3, Title: 'Paper E3', Authors: 'D. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2026', Abstract: '' },
    { DOI: E4, Title: 'Paper E4', Authors: 'E. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2026', Abstract: '' },
    { DOI: E5.toUpperCase(), Title: 'Paper E5', Authors: 'F. Author', Journal: 'EJOR', JKey: 'ejor', Year: '1999', Abstract: '' },
    { DOI: E6, Title: 'Paper E6', Authors: 'G. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2020', Abstract: '' },
    { DOI: E7, Title: 'Paper E7', Authors: 'H. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2021', Abstract: '' },
    { DOI: '10.1016/j.ejor.2019.01.009', Title: 'Paper E9', Authors: 'I. Author', Journal: 'EJOR', JKey: 'ejor', Year: '2019', Abstract: 'x'.repeat(400) },
    { DOI: O1, Title: 'Paper O1', Authors: 'J. Author', Journal: 'QJE', JKey: 'qje', Year: '2026', Abstract: '' },
    { DOI: `https://doi.org/${O2}`, Title: 'Paper O2', Authors: 'K. Author', Journal: 'RFS', JKey: 'rfs', Year: '2026', Abstract: '' },
    { DOI: S1, Title: 'Paper S1', Authors: 'L. Author', Journal: 'MkLett', JKey: 'mlet', Year: '2024', Abstract: '' },
  ];
  writeFileSync(join(DIR, 'papers-ejor.json'), JSON.stringify(rows));
  writeFileSync(join(DIR, '_api-abstracts.json'), JSON.stringify(scen.cache));
  console.log(`  [scenario ${name}]`);
  await main();
  const cache = JSON.parse(readFileSync(join(DIR, '_api-abstracts.json'), 'utf8'));
  const outRows = JSON.parse(readFileSync(join(DIR, 'papers-ejor.json'), 'utf8'));
  scen.check(cache, outRows, { scopusCalls, elsCalls });
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
  const ELS = /^10\.1016\//, SPR = /^10\.(1007|1057|1023)\//;
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
  eq(JSON.stringify(missStamp(1000, 0)), '{"none":1,"t":1000}', 'tier 0 stamps no k (lean file)');
  eq(JSON.stringify(missStamp(1000, 2)), '{"none":1,"t":1000,"k":2}', 'tier 2 stamps k:2');
}

console.log('elsAnswerIsDefinitive: 5xx is not a verdict');
ok(elsAnswerIsDefinitive(200), '200 (with or without text) is a verdict');
ok(elsAnswerIsDefinitive(404), '404 is a verdict');
ok(elsAnswerIsDefinitive(400), '400 is a verdict (the request itself is wrong)');
ok(!elsAnswerIsDefinitive(401) && !elsAnswerIsDefinitive(403) && !elsAnswerIsDefinitive(429), 'the drop-the-leg trio are not verdicts');
ok(!elsAnswerIsDefinitive(500) && !elsAnswerIsDefinitive(503), 'a 5xx leaves the DOI for the next run');

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
  ok(!src.split('\n').some(l => /console\.(log|warn|error)/.test(l) && /\bELS_(INSTTOKEN|KEY)\b/.test(stripBoolUse(l))), 'no log line carries a credential value (a yes/no test of it is fine)');
  ok(/'X-ELS-Insttoken':\s*ELS_INSTTOKEN/.test(src) && /'X-ELS-APIKey':\s*ELS_KEY/.test(src), 'the token is the X-ELS-Insttoken header and the key the X-ELS-APIKey header');
  ok(!/api\.elsevier\.com[^'`]*(apiKey|insttoken)=/i.test(src), 'no Elsevier URL carries apiKey= or insttoken= query parameters');
  ok((src.match(/https:\/\/api\.elsevier\.com/g) || []).length >= 2 && !/http:\/\/api\.elsevier\.com/.test(src), 'every Elsevier endpoint is https');
}

// ── Whole-run scenarios (parent side) ───────────────────────────────────────
console.log('whole-run scenarios against a stubbed fetch (child processes)');
for (const name of ['token', 'scopus-refused', 'key-only', 'scopus-400-fallback', 'quota-spent']) {
  const dir = mkdtempSync(join(tmpdir(), `lit-abs-${name}-`));
  const env = { ...process.env, ABS_SCENARIO: name, FT50_DATA_DIR: dir,
    FT50_ABS_PACE_MS: '150', FT50_ABS_ELS_PACE_MS: '250', FT50_ABS_SCOPUS_PACE_MS: '250', FT50_ABS_SPR_PACE_MS: '250' };
  // A clean credential slate per scenario: only what the scenario sets.
  for (const k of ['ELSEVIER_API_KEY', 'ELSEVIER_INST_TOKEN', 'SPRINGER_API_KEY', 'S2_API_KEY', 'FT50_ABS_SCOPUS', 'FT50_ABS_S2']) delete env[k];
  Object.assign(env, {
    'token': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'scopus-refused': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'key-only': { ELSEVIER_API_KEY: 'key-abc' },
    'scopus-400-fallback': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz' },
    'quota-spent': { ELSEVIER_API_KEY: 'key-abc', ELSEVIER_INST_TOKEN: 'tok-xyz', FT50_ABS_SCOPUS: '0' },
  }[name]);
  const r = spawnSync(process.execPath, [SELF], { env, encoding: 'utf8', timeout: 120000 });
  const lines = `${r.stdout || ''}${r.stderr || ''}`.split('\n').filter(l => /^\s+[✓✗]|scenario/.test(l));
  for (const l of lines) console.log(`  ${l.trim()}`);
  if (r.status !== 0) {
    fails++;
    console.error(`  ✗ scenario ${name} exited ${r.status}`);
    if (!lines.length) console.error((r.stdout || '') + (r.stderr || ''));
  }
  rmSync(dir, { recursive: true, force: true });
}

console.log(fails ? `\nFAILED (${fails})` : '\nAll abstract-backfill checks passed.');
process.exit(fails ? 1 : 0);
