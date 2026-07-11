/*
 * informs-editors.mjs — extract Senior Editor / Associate Editor names from an
 * INFORMS article's "History:" line, e.g.
 *   "History: Dr. Ram D. Gopal, Senior Editor; Dr. Hong Xu, Associate Editor."
 *   "History: Puneet Manchanda served as the senior editor."
 *   "X and Y served as the senior editors for this article."
 * Used for Information Systems Research (SE + AE) and Marketing Science (SE).
 * The text comes either from a Crossref abstract (when INFORMS deposits the
 * History line there) or from the pubsonline article page fetched by
 * informs-editors-local.mjs.
 *
 * Exported: parseInformsEditors(text) -> { se: 'A; B' | '', ae: '...' | '' }
 */

// Keep only what follows the last real sentence boundary (". " after a word,
// not after an initial like "D." or a title like "Dr.").
function tailSegment(s) {
  const parts = String(s || '').split(/(?<=[a-zà-þ]{2})\.\s+/);
  return parts[parts.length - 1];
}

function cleanName(raw) {
  let s = tailSegment(String(raw || ''))
    .replace(/^.*(?:history|editors?)\s*:\s*/i, '')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^(?:and|with|by)\s+/i, '')
    .replace(/^(?:dr|prof(?:essor)?|mr|mrs|ms)\.?\s+/i, '')
    .replace(/[.,;:]+$/, '')
    .trim();
  return s;
}

// A plausible person name: 1-5 tokens, starts uppercase, no sentence words.
function plausibleName(s) {
  if (!s || s.length < 4 || s.length > 60) return false;
  if (s.split(/\s+/).length > 6) return false;
  if (!/^[A-ZÀ-Þ]/.test(s)) return false;
  return !/\b(the|this|that|is|are|was|were|we|of|in|on|to|as|for|paper|article|issue|editors?|received|accepted|revisions?)\b/i.test(s);
}

// "A and B" -> ["A", "B"]; drops implausible pieces.
function splitNames(raw) {
  return String(raw || '')
    .split(/\s+(?:and|&)\s+/i)
    .map(cleanName)
    .filter(plausibleName);
}

export function parseInformsEditors(text) {
  const t = String(text || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
  const se = new Set(), ae = new Set();

  // "<Name>, Senior Editor" / "<Name>, Associate Editor" (";"-separated lists)
  for (const m of t.matchAll(/([^;:]{2,80}?),\s*(?:the\s+)?Senior\s+Editors?\b/gi)) {
    splitNames(m[1]).forEach(n => se.add(n));
  }
  for (const m of t.matchAll(/([^;:]{2,80}?),\s*(?:the\s+)?Associate\s+Editors?\b/gi)) {
    splitNames(m[1]).forEach(n => ae.add(n));
  }
  // "<Name> served as (the) senior editor(s)" — also "… senior editor and
  // <Name> served as associate editor".
  // Periods are allowed inside the capture (initials like "K. Sudhir");
  // cleanName's tailSegment trims anything before a real sentence boundary.
  for (const m of t.matchAll(/([^;:]{2,90}?)\s+served\s+as\s+(?:the\s+)?senior\s+editors?\b/gi)) {
    splitNames(m[1]).forEach(n => se.add(n));
  }
  for (const m of t.matchAll(/([^;:]{2,90}?)\s+served\s+as\s+(?:the\s+)?associate\s+editors?\b/gi)) {
    splitNames(m[1]).forEach(n => ae.add(n));
  }
  // "Senior Editor: <Name>" / "Associate Editor: <Name>"
  let m = t.match(/Senior\s+Editors?\s*[:—-]\s*([^.;]{2,80})/i);
  if (m) splitNames(m[1]).forEach(n => se.add(n));
  m = t.match(/Associate\s+Editors?\s*[:—-]\s*([^.;]{2,80})/i);
  if (m) splitNames(m[1]).forEach(n => ae.add(n));

  return { se: [...se].join('; '), ae: [...ae].join('; ') };
}
