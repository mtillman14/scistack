# Plan: Hybrid schema-key type declaration + canonicalization

> **Status 2026-07-12:** Python implementation COMPLETE (stages 1-3 below,
> plus scifor's `scifor_fatal` abort hatch). MATLAB parity (stage 4) also
> COMPLETE same session: schema_key_types through configure_database,
> `_pathinput_loader` callback seam into the MATLAB scifor loop,
> `load_with_captures` on the MATLAB PathInput wrapper, and canonicalization
> moved to DatabaseManager entry points so the MATLAB bridge is covered.
> Decisions taken: string keys = exact-match only, never manipulated; no
> migration/persistence (testing mode); migration (stage 5) dropped. See
> `docs/claude/schema-key-types.md` for the as-built reference.

## Goal

Minimize syntactic burden: no declaration needed while all path matches are
exact. The first time a numeric-equivalence resolution (`1` → `"001"`) has to
bridge spellings for a key with no declared type, raise an error asking for a
one-time declaration. Once declared, canonicalize that key consistently.

## Core rule (refined from the original proposal)

- **Error is resolution-triggered.** In scidb-managed runs, a PathInput
  numeric-fallback resolution on an undeclared key raises:
  "trial=1 resolved to '001' on disk — declare trial's key type
  (numeric or string) in set_schema to fix its identity."
- **Canonicalization is NOT resolution-triggered — it is unconditional per
  declared key.** Resolution-triggered-only canonicalization would still
  produce mixed spellings: a discovery-driven run literal-matches `"001"`
  (fallback never fires → would save "001") while an explicit `trial=1:12`
  run resolves (→ would save "1"). Once declared numeric, every source —
  discovery captures, DB fills, explicit values — canonicalizes at the
  `_schema_str` boundary.

## Type semantics

| Declared type | Path matching | Stored schema key |
|---|---|---|
| (undeclared) | exact only; numeric fallback raises the declare-error (scidb runs) | verbatim (today's behavior) |
| numeric | numeric-equivalence fallback allowed | unpadded int-string (`"001"` → `"1"`) |
| string | **exact only** — padding is semantic; `"1"` ≢ `"001"` (decision pt 1, recommended) | verbatim, no changes |

Standalone scifor (no database): keeps the silent numeric fallback shipped
2026-07-12 — no stored identity to protect, and scifor stays schema-policy-free
(NOTE 3 layering).

## Stages

1. **scifor seam** — `PathInput.load()` reports resolution events. Add
   `load(..., on_resolve=None)` callback or a richer `resolve_ex()` returning
   `(path, captures, resolved: bool)`; keep `load()` signature/behavior
   byte-compatible for standalone use. scidb's `PerComboLoader` uses the
   richer form.
2. **scidb declaration** — extend `set_schema` with per-key types (exact
   syntax TBD w/ MATLAB parity, e.g. `set_schema([...], types={"trial": "numeric"})`).
   Persist the declaration in the DB alongside the schema so later sessions
   enforce identically. Declaring on a dataset with existing records
   **validates** stored values against canonical form; mismatch → error
   pointing at the migration command (stage 5).
3. **Canonicalization boundary** — apply per-key canonical form in
   `_schema_str` / Step 5 stringification and discovery-combo adoption, so
   Step 2 DB fills, disk combos, and explicit iterables all agree. This also
   fixes the `trial=[]` source-dependent spelling documented in
   `docs/claude/pathinput-zero-padded-matching.md`.
4. **MATLAB parity** — set_schema types through the bridge; declare-error
   translated with a stable error ID; TestPathInput/TestForEach cases.
5. **Migration + docs** — `scidb` CLI Mutator command to canonicalize an
   existing dataset's key (schema-key columns + provenance references;
   follow mutate.py write-seam checklist). Update the zero-padded docs note;
   new docs/claude note for key types.

## Decision points (need user sign-off)

1. String-declared keys: exact-match only (recommended) vs. allow numeric
   resolution but store the disk spelling.
2. `set_schema` declaration syntax (and its MATLAB form).
3. Migration posture: validate-on-declare + explicit CLI migration
   (recommended) vs. auto-migrate on declare.

## Interactions

- Ambiguity ties (`6MWT-1.mat` + `6MWT-001.mat` both on disk) already raise
  from the fallback — unchanged, and string-declared keys sidestep ties
  entirely by never bridging spellings.
- Related open issues to co-resolve or at least not worsen: latest-record
  selection across hashes; content-staleness revisit. Canonical keys make
  both easier, not harder.
