/*
 * _entities.mjs — HTML/JATS entity decoding + markup stripping for the text
 * Crossref and OpenAlex deposit (titles, abstracts).
 * ===========================================================================
 * VENDORED from the site repo's lit/_scraper/_entities.mjs — keep in sync
 * (the same replicated-near-verbatim convention as the pre-print machinery).
 * Publishers deposit titles and abstracts containing HTML/JATS entities and
 * markup, sometimes DOUBLE-encoded ("&amp;lt;p&amp;gt;"). The page HTML-escapes
 * what it renders, so anything left encoded shows up as literal gibberish —
 * "Social Learning and the Innkeeper&apos;s Challenge", "On the Value of
 * R&lt;sup&gt;2&lt;/sup&gt;", "&lt;tocheading&gt;Book Reviews&lt;/tocheading&gt;".
 *
 * The old per-pipeline stripJats had two bugs this module fixes:
 *   1. it decoded ONLY &lt; &gt; &amp;, leaving every other entity raw;
 *   2. it stripped tags BEFORE decoding, so a double-encoded "&lt;sup&gt;2&lt;/sup&gt;"
 *      decoded into literal "<sup>2</sup>" text that nothing then removed.
 * Decoding first (repeatedly), then stripping the revealed tags, then decoding
 * once more is the order the working-papers pipeline's cleanText already used;
 * this module is that algorithm, shared.
 *
 * Deliberately conservative:
 *   • An UNKNOWN named entity is left INTACT — never guessed. A publisher typo
 *     ("&haelip;", "&aacte;") or an exotic MathML name degrades to exactly
 *     today's output instead of turning into confidently-wrong text.
 *   • A tag must start with a letter, so "P &lt; 0.05" survives as "P < 0.05"
 *     and a bare "<http://example.org/>" URL is not eaten as markup.
 *   • <sub>/<sup> strip with NO space, so "Cs<sub>3</sub>Cu<sub>2</sub>I<sub>5</sub>"
 *     stays "Cs3Cu2I5" and "P<sup>2</sup>-FORM" stays "P2-FORM".
 *   • Entity names are matched CASE-SENSITIVELY (&Eacute; is É, &eacute; is é).
 *     An all-caps name that is not itself defined falls back to its capitalised
 *     form, so the all-caps title "VINGT ANS APR&EACUTE;S." yields "APRÉS", not
 *     the lowercase "APRéS" a blind toLowerCase() would give.
 *
 * Pure + idempotent: already-clean text passes through unchanged, so this is
 * safe to (re)apply on every build and over the committed data.
 *
 * Offline test (site repo): node lit/_scraper/entities-selftest.mjs
 * ===========================================================================
 */
import { TITLECASE_WORDS, TITLECASE_PHRASES } from './_titlecase-lexicon.mjs';

// Named entities seen in this catalog plus the common HTML/typography set.
// Add a name here only when its character is certain.
export const HTML_ENTITIES = {
  // core
  amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ',
  // ASCII punctuation JATS deposits by name
  lpar: '(', rpar: ')', lsqb: '[', rsqb: ']', lcub: '{', rcub: '}',
  plus: '+', equals: '=', sol: '/', bsol: '\\', ast: '*', num: '#',
  percnt: '%', commat: '@', excl: '!', quest: '?', colon: ':', semi: ';',
  period: '.', comma: ',', dollar: '$', lowbar: '_', verbar: '|', grave: '`',
  // dashes, quotes, typography
  ndash: '–', mdash: '—', horbar: '―', hyphen: '‐', hellip: '…',
  lsquo: '‘', rsquo: '’', ldquo: '“', rdquo: '”',
  sbquo: '‚', bdquo: '„', prime: '′', Prime: '″',
  bull: '•', middot: '·', sect: '§', para: '¶', dagger: '†', Dagger: '‡',
  copy: '©', reg: '®', trade: '™', permil: '‰', ordm: 'º', ordf: 'ª',
  laquo: '«', raquo: '»', lsaquo: '‹', rsaquo: '›', shy: '­',
  // currency
  pound: '£', euro: '€', yen: '¥', cent: '¢', curren: '¤',
  // maths / symbols
  times: '×', divide: '÷', minus: '−', plusmn: '±', deg: '°', micro: 'µ',
  frac12: '½', frac14: '¼', frac34: '¾', sup1: '¹', sup2: '²', sup3: '³',
  ne: '≠', le: '≤', ge: '≥', les: '≤', ges: '≥', ll: '≪', gg: '≫',
  asymp: '≈', equiv: '≡', prop: '∝', infin: '∞', radic: '√', sum: '∑',
  prod: '∏', int: '∫', part: '∂', nabla: '∇', isin: '∈', notin: '∉',
  ni: '∋', cap: '∩', cup: '∪', sub: '⊂', sup: '⊃', sube: '⊆', supe: '⊇',
  and: '∧', or: '∨', not: '¬', forall: '∀', exist: '∃', empty: '∅',
  setmn: '∖', ctdot: '⋯', cdot: '⋅', cdots: '⋯', hellips: '…', cir: '○',
  lowast: '∗', sdot: '⋅', oplus: '⊕', otimes: '⊗', perp: '⊥', ang: '∠',
  larr: '←', rarr: '→', uarr: '↑', darr: '↓', harr: '↔', rArr: '⇒', lArr: '⇐',
  hArr: '⇔', there4: '∴', alefsym: 'ℵ', real: 'ℜ', image: 'ℑ', weierp: '℘',
  Escr: 'ℰ', Copf: 'ℂ', Nopf: 'ℕ', Qopf: 'ℚ', Ropf: 'ℝ', Zopf: 'ℤ',
  // Greek (lower)
  alpha: 'α', beta: 'β', gamma: 'γ', delta: 'δ', epsilon: 'ε', zeta: 'ζ',
  eta: 'η', theta: 'θ', thetasym: 'ϑ', iota: 'ι', kappa: 'κ', lambda: 'λ',
  mu: 'μ', nu: 'ν', xi: 'ξ', omicron: 'ο', pi: 'π', piv: 'ϖ', rho: 'ρ',
  sigma: 'σ', sigmaf: 'ς', tau: 'τ', upsilon: 'υ', phi: 'φ', phiv: 'ϕ',
  chi: 'χ', psi: 'ψ', omega: 'ω',
  // Greek (upper)
  Alpha: 'Α', Beta: 'Β', Gamma: 'Γ', Delta: 'Δ', Epsilon: 'Ε', Zeta: 'Ζ',
  Eta: 'Η', Theta: 'Θ', Iota: 'Ι', Kappa: 'Κ', Lambda: 'Λ', Mu: 'Μ', Nu: 'Ν',
  Xi: 'Ξ', Omicron: 'Ο', Pi: 'Π', Rho: 'Ρ', Sigma: 'Σ', Tau: 'Τ',
  Upsilon: 'Υ', Phi: 'Φ', Chi: 'Χ', Psi: 'Ψ', Omega: 'Ω',
  // Latin-1 letters (lower)
  agrave: 'à', aacute: 'á', acirc: 'â', atilde: 'ã', auml: 'ä', aring: 'å',
  aelig: 'æ', ccedil: 'ç', egrave: 'è', eacute: 'é', ecirc: 'ê', euml: 'ë',
  igrave: 'ì', iacute: 'í', icirc: 'î', iuml: 'ï', eth: 'ð', ntilde: 'ñ',
  ograve: 'ò', oacute: 'ó', ocirc: 'ô', otilde: 'õ', ouml: 'ö', oslash: 'ø',
  ugrave: 'ù', uacute: 'ú', ucirc: 'û', uuml: 'ü', yacute: 'ý', thorn: 'þ',
  yuml: 'ÿ', szlig: 'ß', inodot: 'ı', imath: 'ı', osol: 'ø', Osol: 'Ø',
  // Latin-1 letters (upper)
  Agrave: 'À', Aacute: 'Á', Acirc: 'Â', Atilde: 'Ã', Auml: 'Ä', Aring: 'Å',
  AElig: 'Æ', Ccedil: 'Ç', Egrave: 'È', Eacute: 'É', Ecirc: 'Ê', Euml: 'Ë',
  Igrave: 'Ì', Iacute: 'Í', Icirc: 'Î', Iuml: 'Ï', ETH: 'Ð', Ntilde: 'Ñ',
  Ograve: 'Ò', Oacute: 'Ó', Ocirc: 'Ô', Otilde: 'Õ', Ouml: 'Ö', Oslash: 'Ø',
  Ugrave: 'Ù', Uacute: 'Ú', Ucirc: 'Û', Uuml: 'Ü', Yacute: 'Ý', THORN: 'Þ',
};

// Resolve one named entity, case-sensitively. An all-caps name that is not
// itself defined ("&EACUTE;") falls back to its capitalised form ("Eacute" -> É),
// so an all-caps title decodes to an upper-case letter rather than a lower-case
// one. Returns undefined when the name is unknown — the caller leaves it intact.
export function namedEntity(name) {
  if (Object.prototype.hasOwnProperty.call(HTML_ENTITIES, name)) return HTML_ENTITIES[name];
  if (name === name.toUpperCase() && name !== name.toLowerCase()) {
    const cap = name[0] + name.slice(1).toLowerCase();
    if (Object.prototype.hasOwnProperty.call(HTML_ENTITIES, cap)) return HTML_ENTITIES[cap];
    const low = name.toLowerCase();
    if (Object.prototype.hasOwnProperty.call(HTML_ENTITIES, low)) return HTML_ENTITIES[low];
  }
  return undefined;
}

export function decodeEntitiesOnce(s) {
  return s.replace(/&(#x[0-9a-f]+|#\d+|[a-z][a-z0-9]*);/gi, (m, ent) => {
    if (ent[0] === '#') {
      const cp = (ent[1] === 'x' || ent[1] === 'X')
        ? parseInt(ent.slice(2), 16) : parseInt(ent.slice(1), 10);
      if (Number.isFinite(cp) && cp > 0 && cp <= 0x10ffff) {
        try { return String.fromCodePoint(cp); } catch { return m; }
      }
      return m;
    }
    const v = namedEntity(ent);
    return v === undefined ? m : v; // leave unknown named entities intact
  });
}

// Decode entities (repeatedly, so a double-encoding fully resolves), strip the
// markup that decoding reveals, then decode once more for anything a tag hid.
export function cleanText(raw) {
  let s = String(raw == null ? '' : raw);
  if (!/[&<]/.test(s)) return s.replace(/\s+/g, ' ').trim(); // fast path
  for (let i = 0; i < 6; i++) { const d = decodeEntitiesOnce(s); if (d === s) break; s = d; }
  s = s
    .replace(/<\/?(?:sub|sup)(?:\s[^<>]*)?\/?>/gi, '')          // no space: Cs3Cu2I5, P2-FORM
    .replace(/<\/?[a-z][a-z0-9:-]*(?:\s[^<>]*)?\/?>/gi, ' ');   // other tags -> space
  for (let i = 0; i < 3; i++) { const d = decodeEntitiesOnce(s); if (d === s) break; s = d; }
  return s.replace(/\s+/g, ' ').trim();
}

// A stray trailing separator on a title/affiliation is a publisher deposit
// artifact (feedback ticket LIT-260725-YWTL). Trim one at a time, so " ,", ",,"
// and ", ;" all collapse — but NEVER the ';' that terminates an entity cleanText
// left intact ("…&haelip;"), since cutting that would corrupt it further.
export function trimTrailingSeparators(raw) {
  let s = String(raw == null ? '' : raw).replace(/\s+$/, '');
  while (/[,;:]$/.test(s)) {
    if (s.endsWith(';') && /&(?:#x[0-9a-f]+|#\d+|[a-z][a-z0-9]*);$/i.test(s)) break;
    s = s.slice(0, -1).replace(/\s+$/, '');
  }
  return s;
}

// Article-page furniture must never be served as an abstract (feedback ticket
// LIT-260727-XRQ8). Two junk shapes reach the pipelines: (1) the pubsonline
// page harvest can capture the navigation + "Cited by" block AFTER the real
// abstract ("… Previous Back to Top Next Figures References Related
// Information Cited by <citing-article list>"), and (2) Semantic Scholar
// sometimes serves a scrape of the WHOLE article page — share bar, author
// links, citation metadata — for items that have no abstract at all
// ("Journal Article <title> Get access <authors> Search for other works by
// this author on: Oxford Academic Google Scholar …", U. Chicago's "Previous
// articleNext article … PDFPDF PLUS Add to favoritesDownload Citation…").
// stripPageFurniture cuts everything from the first navigation signature on,
// then rejects (→ '') a remainder that is page chrome rather than abstract
// prose. Deliberately high-precision, like isNonArticle: the cut anchors on
// the full "Figures References Related Information" section-label sequence —
// never on "Back to Top" alone, which could occur inside a real sentence —
// and the chrome test needs either one unambiguous marker (phrases that can
// only come from a scraped page) or two weaker ones together. Weak markers
// alone never reject, so an abstract that legitimately says e.g. "…request
// permission…" survives. Pure + idempotent; callers keep their existing
// ≥60-char floor for what counts as a real abstract.
const FURNITURE_CUT_RE =
  /(?:(?:Previous\s+)?Back to Top\s+)?(?:Next\s+)?Figures\s*References\s*Related\s*Information/i;
const CHROME_STRONG_RE = [
  /ShareShare on/i,
  /Share on\s*Facebook/i,
  /PDFPDF/,
  /AboutSectionsView/i,
  /Previous articleNext article/i,
  /Download citation file/i,
  /Search for other works by this author/i,
  /Search for more papers by this author/i,
  /Crossref reports no articles citing this article/i,
  /No abstract is available for this article/i,
  /PermissionsReprints/,
  /View PDF Tools/i,
  /This article corrects the following/i,
];
const CHROME_WEAK_RE = [
  /Add to favorites/i,
  /Track citations?\b/i,
  /Download citations?\b/i,
  /Published Online\s*:/i,
  /Article Information\s*Metrics/i,
  /Request permissions?\b/i,
  /Export citation\b/i,
  /First published\s*:/i,
];
export function stripPageFurniture(raw) {
  let s = String(raw == null ? '' : raw);
  const i = s.search(FURNITURE_CUT_RE);
  if (i >= 0) s = s.slice(0, i).trim();
  if (CHROME_STRONG_RE.some((re) => re.test(s))) return '';
  if (CHROME_WEAK_RE.filter((re) => re.test(s)).length >= 2) return '';
  return s;
}

// An ALL-CAPITALS title is a deposit artifact, not how the paper reads
// (feedback ticket LIT-260728-TVQ5). Older registrations shout — PNAS's
// pre-1970s back-catalogue, POM's Wiley years, JoF, TAR, AMJ, IER, Economic
// Inquiry: "MARKET EQUILIBRIUM", "A PENTAPLOID LARVA OF THE NEWT, TRITURUS
// VIRIDESCENS". Beyond reading badly on a card it wrecks the BibTeX export,
// which brace-protects every capital after the first, so the user copies
// "M{A}{R}{K}{E}{T} {E}{Q}{U}{I}{L}{I}{B}{R}{I}{U}{M}" into their .bib.
//
// Scope, per the owner: ONLY all-capitals titles are rewritten. A Title Case
// title ("Modeling First: Rethinking Undergraduate Operations Management with
// AI") is how it was published and is left EXACTLY alone — which is why
// isAllCapsTitle demands the string contain no lowercase letter at all rather
// than measuring some ratio.
//
// Restoring case is not toLowerCase(): acronyms (DNA, CAPM, ANOVA) and proper
// nouns (Bayesian, Cournot, Drosophila, Japan) must keep their capitals, and an
// all-caps string carries no evidence of which word is which. That evidence
// comes from _titlecase-lexicon.mjs, mined from the ~715k properly-cased titles
// and ~306k abstracts the catalog already holds (see
// build-titlecase-lexicon.mjs). Anything the lexicon does not know is
// lowercased — the sentence-case default, and the safe direction: a missed
// proper noun reads as a small blemish, whereas capitalising by guess would
// invent names. The import is deliberately STATIC so a shard repo that vendored
// _entities.mjs without the lexicon fails loudly at import instead of silently
// lowercasing every acronym in the catalog.
const TC_TOKEN_RE = /[\p{L}\p{N}][\p{L}\p{N}'’]*/gu;
// Canonical Roman numeral. It also matches ordinary words ("MIX" = M + IX), so
// it is only ever consulted right after a numbering word.
const TC_ROMAN_RE = /^M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$/;
const TC_NUMBERING = new Set(['volume', 'vol', 'part', 'chapter', 'chap', 'no', 'number', 'book',
  'series', 'section', 'appendix', 'table', 'figure', 'fig', 'phase', 'type', 'class', 'war',
  'level', 'stage', 'study', 'experiment']);

// True only for a title that is entirely upper-case, long enough to be prose
// rather than a bare acronym, and carries at least one word the lexicon does
// NOT know as an acronym — so "DNA" or "IEEE ACM" is never "corrected", while
// "DNA AND RNA" is (the word "and" qualifies).
export function isAllCapsTitle(s) {
  const t = String(s == null ? '' : s);
  if (/\p{Ll}/u.test(t)) return false;
  if ((t.match(/\p{Lu}/gu) || []).length < 8) return false;
  const words = (t.match(TC_TOKEN_RE) || []).filter((w) => /\p{L}/u.test(w));
  if (words.length < 2) return false;
  return words.some((w) => w.length >= 3 && TITLECASE_WORDS[w.toLowerCase()] !== w.toUpperCase());
}

function tcCapFirst(w) { return w[0].toUpperCase() + w.slice(1); }

// One word's restored form. prevKey/nextCh/prevCh give the little context the
// ambiguous cases need.
function tcRecaseWord(w, prevKey, nextCh, prevCh) {
  const k = w.toLowerCase();
  if (Object.prototype.hasOwnProperty.call(TITLECASE_WORDS, k)) return TITLECASE_WORDS[k];
  // A possessive is the same word wearing an apostrophe: "CFPB'S" -> "CFPB's",
  // "NEUMANN'S" -> "Neumann's". The lexicon stores whichever form it saw, so
  // fall back to the base word when only that is known.
  const poss = /^(.+)(['’])([Ss])$/.exec(w);
  if (poss) {
    const base = TITLECASE_WORDS[poss[1].toLowerCase()];
    if (base) return base + poss[2] + poss[3].toLowerCase();
  }
  if (/^\d+(ST|ND|RD|TH)$/.test(w)) return w.toLowerCase();          // 21ST -> 21st
  // A word carrying a trailing footnote/superscript digit ("CONTROLLERS1"). The
  // 4-letter floor keeps short codes — T2, M12, CO2 — out of this branch.
  const foot = /^(\p{L}{4,})(\d+)$/u.exec(w);
  if (foot) {
    const base = foot[1].toLowerCase();
    return (TITLECASE_WORDS[base] || base) + foot[2];
  }
  if (/\p{N}/u.test(w)) return w;                                     // a code: keep verbatim
  if (w.length === 1) {
    // "A" is nearly always the article; a single letter followed by "." or
    // joined by "-" is an initial or a symbol ("E. COLI", "S-CURVE"), as is any
    // other lone letter ("ON THE VALUE OF X").
    if (k === 'a' && nextCh !== '.' && nextCh !== '-' && prevCh !== '-') return 'a';
    return w;
  }
  if (TC_ROMAN_RE.test(w) && TC_NUMBERING.has(prevKey)) return w;     // "VOLUME CXXI"
  return k;
}

// Rewrite an ALL-CAPS title into sentence case: first letter up, the rest down
// except acronyms/proper nouns, and a capital again after ':' (also '?', '!'
// and a sentence-ending '.'). Any other title is returned untouched, so this is
// pure + idempotent and safe to re-apply on every build.
export function sentenceCaseTitle(s) {
  const t = String(s == null ? '' : s);
  if (!isAllCapsTitle(t)) return t;
  const ms = [...t.matchAll(TC_TOKEN_RE)];
  // Multi-word names first, longest run wins: the parts of "UNITED STATES" are
  // lowercase-dominant on their own, so word-by-word gives "United states".
  const fixed = new Array(ms.length).fill(null);
  for (let i = 0; i < ms.length; i++) {
    if (fixed[i]) continue;
    for (const n of [3, 2]) {
      if (i + n > ms.length) continue;
      const val = TITLECASE_PHRASES[ms.slice(i, i + n).map((m) => m[0].toLowerCase()).join(' ')];
      if (!val) continue;
      const parts = val.split(' ');
      if (parts.length !== n) continue;
      for (let j = 0; j < n; j++) fixed[i + j] = parts[j];
      i += n - 1;
      break;
    }
  }
  let out = '', last = 0, boundary = true, prevWord = '', prevKey = '';
  for (let i = 0; i < ms.length; i++) {
    const w = ms[i][0], gap = t.slice(last, ms[i].index);
    out += gap;
    // Quotes and brackets may sit on either side of the punctuation — a closing
    // one before it ("…HIGHER EDUCATION?’ AN ECONOMIC…"), an opening one after
    // it — so any run of them still leaves the next word starting a sentence.
    if (last !== 0 && /[:?!][\s"“”'‘’()[\]]*$/.test(gap)) boundary = true;
    // A '.' ends a sentence only after a real word — never after an initial,
    // so "E. COLI" stays "E. coli" and "U.S." is left alone.
    else if (last !== 0 && /\.[\s"“”'‘’()[\]]*$/.test(gap) && prevWord.length >= 2) boundary = true;
    const end = ms[i].index + w.length;
    let form = fixed[i] || tcRecaseWord(w, prevKey, t.charAt(end), t.charAt(ms[i].index - 1));
    if (boundary && /\p{Ll}/u.test(form[0])) form = tcCapFirst(form);
    out += form;
    prevWord = w; prevKey = w.toLowerCase(); boundary = false; last = end;
  }
  return out + t.slice(last);
}

// A title as served: markup/entities cleaned, any dangling separator trimmed,
// then an all-capitals deposit brought back to sentence case.
export function titleText(s) { return sentenceCaseTitle(trimTrailingSeparators(cleanText(s))); }

// One affiliation name as served (the page splits Affiliations on ';').
export function affilName(s) { return trimTrailingSeparators(cleanText(s)); }

// A whole Affiliations string as served: clean each ';'-separated name and drop
// any that collapses to nothing (an empty segment is what leaves a "; ;" gap).
// The ';' that TERMINATES an entity is not a separator, so those are masked out
// before the split and restored after.
export function affilParts(s) {
  const str = String(s == null ? '' : s);
  if (!str) return [];
  const MASK = '\u0001';
  const masked = str.replace(/&(?:#x[0-9a-f]+|#\d+|[a-z][a-z0-9]*);/gi, (m) => m.slice(0, -1) + MASK);
  return masked.split(';')
    .map((seg) => affilName(seg.split(MASK).join(';')))
    .filter(Boolean);
}
// Iterated to a fixed point: a DOUBLE-encoded entity in an affiliation
// ("&amp;#38;") decodes one layer per pass — the mask sees only the outer
// entity — so one application can leave "&#38;" behind; a couple more settle it.
export function affilList(s) {
  let cur = affilParts(s).join('; ');
  for (let i = 0; i < 3; i++) {
    const next = affilParts(cur).join('; ');
    if (next === cur) break;
    cur = next;
  }
  return cur;
}
