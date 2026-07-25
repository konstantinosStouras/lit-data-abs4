/*
 * _entities.mjs — HTML/JATS entity decoding + markup stripping for the text
 * Crossref and OpenAlex deposit (titles, abstracts).
 * VENDORED from the site repo's lit/_scraper/_entities.mjs — keep in sync
 * (the same replicated-near-verbatim convention as the pre-print machinery).
 * ===========================================================================
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

// A title as served: markup/entities cleaned, then any dangling separator trimmed.
export function titleText(s) { return trimTrailingSeparators(cleanText(s)); }

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
