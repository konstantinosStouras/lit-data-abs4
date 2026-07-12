# lit-data-abs4

A **satellite data shard** for [stouras.com/fun/lit/](https://www.stouras.com/fun/lit/)
("The Lit" research paper browser): ABS 4/4* journals beyond the FT50/UTD24
lists — **Operations / Supply Chain / Economics / Computer Science-related
fields only** (the lit catalog covers FT50/UTD24 in full, all fields, but
ABS coverage beyond those lists is deliberately limited to these fields;
out-of-scope journals are marked `"retired": true`, never harvested).

GitHub Pages caps a published site at 1 GB, so the lit catalog is sharded
across data repos. This repo:

- carries its own journal list in `_scraper/journals.json` (each entry's
  `abs` grade per the Chartered ABS Academic Journal Guide 2024, as mirrored
  at [journalranking.org](https://journalranking.org/));
- harvests every journal's full history from the Crossref API via
  `_scraper/build-data.mjs` (an adapted copy of the main repo's proven
  `fun/lit/_scraper-ft50` pipeline), daily via
  `.github/workflows/update-data.yml`;
- publishes `/data/` on this repo's GitHub Pages site — served under
  `https://www.stouras.com/lit-data-abs4/data/`, the same origin as the lit
  page, which merges this shard's `data/sources.json` manifest at runtime
  and lazy-loads each `papers-<key>.json` only when a filter needs it.

**To add a journal:** verify its ABS grade on journalranking.org, append an
entry to `_scraper/journals.json` (key, name, ISSNs, publisher, `abs`, and
`aia: true` if it has an advance-publication stage), and push — the workflow
harvests it and the lit page picks it up automatically. See
`JOURNALS-TODO.md` for pre-researched candidates awaiting grade confirmation.

**To remove a journal:** set `"retired": true` on its entry; the next build
deletes its data file and drops it from the manifest.

Requires GitHub Pages enabled on this repo (Settings → Pages → Deploy from a
branch → `main` / root).
