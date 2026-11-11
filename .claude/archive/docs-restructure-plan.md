# SciStack Docs Restructure — Plan

**Scope of this document:** page *structure* only — the navigation layout (how
menus render), the section/title hierarchy, and the methodology for getting
content *correct*. No prose is written yet. Host target: **ReadTheDocs only**.

**Ground-truth rule:** The existing docs (`docs/*.md`, `docs/guide/*`,
`docs/api/*`, `README.md`, `docs/index.md`) are **not trusted**. They convey the
right conceptual *intent* per package but are largely stale on API specifics.
The **package `tests/` directories are the source of truth.** Every page must be
reconciled against the relevant test suite before its prose is finalized.

---

## 1. Navigation & layout design (mkdocs-material)

### The three-pane layout

We adopt the classic mkdocs-material three-pane reading experience:

```
┌───────────────────────────────────────────────────────────────┐
│  HEADER:  SciStack   [ Getting Started | Concepts | User Guide │  ← top tabs
│                        | Walkthroughs | API | Project ]  🔍     │    (one per
├──────────────┬─────────────────────────────────┬──────────────┤     top-level
│ LEFT SIDEBAR │        PAGE CONTENT             │ RIGHT TOC    │     area)
│ (current     │                                 │ (this page's │
│  tab's pages,│                                 │  headings)   │
│  collapsible │                                 │              │
│  dropdowns)  │                                 │              │
└──────────────┴─────────────────────────────────┴──────────────┘
```

- **Top horizontal tabs** = the top-level areas. Chosen because the IA has 7
  areas; putting them across the top keeps the left sidebar scoped to *only the
  active area*, so a newcomer is never confronted with the entire site at once.
- **Left sidebar** = the pages within the active tab, with nested groups as
  **collapsible dropdowns** (collapsed by default, except the current section).
- **Right sidebar** = the in-page table of contents (the current page's
  headings). Kept on the right (default), NOT integrated into the left.

### `mkdocs.yml` `theme.features` changes

Current:
```yaml
features:
  - navigation.sections     # renders groups as flat bold headers (no collapse)
  - navigation.expand       # force-expands everything
  - content.code.copy
  - content.tabs.link
```

Proposed:
```yaml
features:
  - navigation.tabs          # NEW: top-level areas become header tabs
  - navigation.tabs.sticky   # NEW: tabs stay visible when scrolling
  - navigation.top           # NEW: back-to-top button
  - navigation.footer        # NEW: prev/next page links at page bottom
  - navigation.indexes       # NEW: lets a section have its own landing page
  - toc.follow               # NEW: right TOC auto-scrolls to active heading
  - content.code.copy        # keep: copy button on code blocks
  - content.code.annotate    # NEW: inline code annotations (good for examples)
  - content.tabs.link        # keep: linked Python/MATLAB tabs across pages
  # REMOVED navigation.sections + navigation.expand:
  #   with 7 tabs the sidebar should be collapsible per-area, not flat+expanded.
```

Rationale for the two removals: `navigation.sections` + `navigation.expand`
render the *entire* tree flat and open. That was tolerable for ~5 groups; with
the expanded IA it produces an overwhelming wall of links. Tabs + collapsible
dropdowns scale far better.

### Python / MATLAB content tabs

The product is dual-language. Every code example that differs between languages
uses pymdownx tabbed blocks:

```markdown
=== "Python"
    ```python
    ...
    ```
=== "MATLAB"
    ```matlab
    ...
    ```
```

`content.tabs.link` (already enabled) makes all such tabs switch together — pick
"MATLAB" once and the whole page follows. This is our standard for every example.

### Markdown extensions

Keep the current set; add a few that the content will need:
- keep: `pymdownx.highlight`, `pymdownx.superfences`, `pymdownx.tabbed`,
  `admonition`, `toc` (permalink), `attr_list`.
- add: `pymdownx.details` (collapsible admonitions for FAQ/troubleshooting),
  `pymdownx.tasklist` (roadmap checklists), `tables`, `md_in_html`,
  `pymdownx.emoji` (status icons in roadmap/parity tables).

### Section index pages (`navigation.indexes`)

Each top-level area gets an `index.md` acting as its landing page, so clicking a
tab lands somewhere useful instead of jumping to the first child. Areas that get
an index page: Concepts, User Guide, API Reference, Project.

---

## 2. Full section / title hierarchy (the `nav:` tree)

Organized on Diátaxis: **Getting Started** (orientation) · **Concepts**
(understanding) · **User Guide** (doing) · **Walkthroughs** (learning) · **API
Reference** (lookup) · **Project** (meta). Concepts and User Guide are kept
**separate** per decision.

```
Home                                    index.md
                                        (REWRITE — current one is stale: still
                                         pitches single-package scidb + @thunk +
                                         SQLite; must tell the layered story)

Getting Started/
  Installation                          getting-started/installation.md      (NEW)
  Quickstart                            quickstart.md                        (REWRITE vs tests)
  Choosing Your Layer                   getting-started/choosing-a-layer.md  (NEW)
  MATLAB Setup                          matlab-setup.md                      (verify)

Concepts/
  Overview                              concepts/index.md                    (NEW, section index)
  Architecture & Layers                 concepts/architecture.md             (NEW)
  Variables & Storage                   concepts/variables.md                (NEW/derive)
  Lineage & Provenance                  concepts/lineage.md                  (NEW/derive)
  Computation Caching                   concepts/caching.md                  (NEW/derive)
  Node States                           guide/node-states.md  → concepts/node-states.md  (ADOPT orphan)
  Versioning & Content Hashing          concepts/hashing.md                  (NEW)
  Glossary                              concepts/glossary.md                 (NEW)

User Guide/
  Overview                              guide/index.md                       (NEW, section index)
  Defining Variables                    guide/variables.md                   (REWRITE vs tests)
  Database & Configuration              guide/database.md                    (REWRITE vs tests)
  Tracking Lineage                      guide/lineage.md                     (REWRITE vs tests)
  Caching Computations                  guide/caching.md                     (REWRITE vs tests)
  Batch Processing (for_each)           guide/for_each.md                    (REWRITE vs tests)
  Filtering & Selection                 (from orphan api/filters.md)         (RELOCATE/REWRITE)
  Browsing & Exporting                  guide/browsing.md                    (REWRITE vs tests)

Walkthroughs/
  VO2 Max Pipeline                      guide/walkthrough.md                 (REWRITE vs tests)

API Reference/
  Overview                              api/index.md                         (verify)
  Variables (BaseVariable)              api/variables.md                     (REWRITE vs tests)
  Database                              api/database.md                      (REWRITE vs tests)
  Lineage (Thunk System)                api/lineage.md                       (REWRITE vs tests)
  Batch Processing (for_each)           api/for-each.md                      (REWRITE vs tests)
  Filters                               api/filters.md                       (ADOPT orphan, verify)

Project/
  Roadmap                               docs/future_ideas.md → project/roadmap.md  (ADOPT orphan)
  Contributing & Dev Setup              project/contributing.md              (NEW)
  Building These Docs                   project/building-docs.md             (NEW)
  FAQ & Troubleshooting                 project/faq.md                       (NEW)
```

Notes:
- "Choosing Your Layer" is the single highest-value new page — the product pitch
  is "enter at any level" but nothing today helps a reader pick a level.
- Stale/duplicate files to retire once content is migrated: `docs/api.md` (flat,
  superseded by `api/`), `docs/README_old.md`.
- Changelog deferred until releases are tagged.

---

## 3. Content-correction methodology (tests as ground truth)

Each content page below is bound to the test files that define its real,
current behavior. When writing/rewriting a page, read those tests first; if the
old doc disagrees with the tests, the **tests win**. Discrepancies get logged in
section 4.

| Page | Ground-truth tests |
|---|---|
| Architecture & Layers | layering implied by imports across all suites; `scihist/tests/test_foreach.py`, `scidb/tests/test_integration.py` |
| Variables & Storage / API Variables | `scidb/tests/test_integration.py`, `test_introspect.py`, `test_constant.py`, `sciduckdb/tests/test_sciduck.py` |
| Database & Configuration / API Database | `scidb/tests/test_integration.py`, `test_discover.py`, `test_orphaned_records.py`, `test_load_all_ordering.py`, `sciduckdb/tests/test_sciduck.py` |
| Lineage & Provenance / API Lineage | `scilineage/tests/test_lineage.py`, `test_core.py`, `test_hashing.py`; `scidb/tests/test_optional_lineage_dependency.py` |
| Caching / Node States | `scihist/tests/test_cache_hit.py`, `test_skip_computed.py`, `test_state*.py` (8 files), `scidb/tests/test_call_id.py` |
| Versioning & Content Hashing | `scicanonicalhash/tests/test_hashing.py`, `scilineage/tests/test_hashing.py`, `scidb/tests` hashing-related |
| Batch Processing (for_each) | `scifor/tests/test_foreach_standalone.py`, `test_schema.py`, `test_merge_*.py`; `scidb/tests/test_for_columns.py`, `test_each_of.py`, `test_aggregation*.py`, `test_variant_*.py`; `scihist/tests/test_foreach.py`, `test_fixed.py`, `test_merge.py`, `test_unified_variant_tracking.py` |
| Filtering & Selection / API Filters | `scifor/tests/test_filters.py`; `scidb/tests/test_filters.py`, `test_where.py`, `test_exclusions.py`, `test_schema_key_filter.py`, `test_variable_filter_merge.py` |
| Browsing & Exporting | `scifor/tests/test_merge_as_df.py`, `test_merge_to_csv.py`; `scidb/tests/test_to_csv.py` |
| MATLAB Setup / parity | `scimatlab/tests/test_bridge*.py`; `scihist/tests/test_state_matlab_pathinput.py` |
| Path inputs (Walkthrough/for_each detail) | `scifor/tests/test_pathinput_discover.py`, `test_pathinput_regex.py`; `path-gen/tests/test_generator.py`; `scihist/tests/test_state_pathinput.py`, `test_generates_file.py` |
| scidb-net (optional layer) | `scidb-net/tests/test_serialization.py` |

Current package names confirmed on disk: `scifor`, `scidb`, `scilineage`,
`scihist`, `sciduckdb`, `scicanonicalhash`, `path-gen` (scipathgen),
`scimatlab`, `scidb-net`. The API names used throughout the existing docs
(`configure_database`, `set_schema`, `@thunk` vs `@lineage_fcn`, `Thunk()`,
`Fixed`, `for_each`) must each be re-verified against the tests above — the
README and index.md already disagree with each other on several.

---

## 4. Reconciliation backlog (to fill during writing)

Known issues to resolve against tests when content work begins:
- index.md tells the OLD single-package story; README tells the NEW layered
  story. Neither is verified against tests. → reconcile, rewrite index.md.
- README internal inconsistencies: `@thunk` vs `@lineage_fcn`; `set_schema` vs
  `configure_database`; broken/duplicated code fences (~L99-110, L376-390);
  "Layer 2" header with no "Layer 1".
- Orphaned-but-written pages to adopt: `guide/node-states.md`,
  `api/filters.md`, `docs/future_ideas.md`.
- Files to retire after migration: `docs/api.md`, `docs/README_old.md`.

---

## 5. Local testing workflow (already works)

`mkdocs` + `mkdocs-material` are installed in `.venv`:
- `\.venv/bin/mkdocs serve` — live preview at http://127.0.0.1:8000
- `\.venv/bin/mkdocs build --strict` — fails on broken nav/links (CI gate)

`.readthedocs.yaml` is already configured (Ubuntu 22.04, Python 3.11,
`docs/requirements.txt`). Remaining hosting step: connect the RTD project to the
GitHub repo (one-time, in the RTD dashboard).
```
