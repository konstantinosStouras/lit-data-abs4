/*
 * abstracts-selftest.mjs — offline tests (no network) for this shard's API
 * abstract backfill (abstracts-ci.mjs, vendored from the site repo's
 * lit/_scraper-ft50/ — keep in sync): OpenAlex inverted-index reconstruction,
 * the cache-merge rule, the needy-row test and the Elsevier parser.
 * Run: node _scraper/abstracts-selftest.mjs
 */
import { invertedToText, mergeAbsCache, isNeedy, elsevierAbstract, springerAbstract,
  betterAbstract } from './abstracts-ci.mjs';

let fails = 0;
const ok = (c, m) => { if (c) console.log(`  ✓ ${m}`); else { console.error(`  ✗ ${m}`); fails++; } };
const eq = (g, w, m) => ok(g === w, `${m}${g === w ? '' : `  (got ${JSON.stringify(g)}, want ${JSON.stringify(w)})`}`);

console.log('invertedToText: OpenAlex abstract_inverted_index → text');
eq(invertedToText({ We: [0], study: [1], markets: [2, 4], in: [3] }),
  'We study markets in markets', 'positions reassembled in order, repeated words placed twice');
eq(invertedToText({}), '', 'empty index → empty string');
eq(invertedToText(null), '', 'null index → empty string');
eq(invertedToText({ a: 'junk' }), '', 'malformed positions ignored');

console.log('mergeAbsCache: abstract beats none, longer beats shorter');
let c = { d1: { none: 1, t: 1 }, d2: { a: 'short' }, d3: { a: 'keep me intact' } };
const took = mergeAbsCache(c, { d1: { a: 'a full abstract text' }, d2: { a: 'a much longer abstract' }, d3: { none: 1, t: 9 }, d4: { a: 'brand new' } });
eq(took, 3, 'three entries taken (none→a, shorter→longer, new)');
eq(c.d1.a, 'a full abstract text', 'none upgraded to abstract');
eq(c.d2.a, 'a much longer abstract', 'shorter upgraded to longer');
eq(c.d3.a, 'keep me intact', 'a none-record never downgrades an abstract');
eq(c.d4.a, 'brand new', 'new entry taken');

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

console.log('betterAbstract import path (shared upgrade rule)');
ok(betterAbstract('', 'y'.repeat(80)), 'empty ← candidate works via the cross-import');

console.log(fails ? `\nFAILED (${fails})` : '\nAll FT50 abstract-backfill checks passed.');
process.exit(fails ? 1 : 0);
