# Plan: schema key types (numeric / string) stored in the database, settable from the GUI

## Problem

Trial numbers show up as `"001"`, `"1"` and `1`. scidb already handles this:
a key declared `"numeric"` is converted unconditionally (`"001"` → `1`) wherever
a value enters or leaves storage (`DatabaseManager.canonicalize_metadata`;
`docs/claude/schema-key-types.md`). But the only way to declare it is
`configure_database(..., schema_key_types=...)`, and the GUI opens the database
without that argument (`scistack_gui/db.py:549,579`). So in the GUI every key is
undeclared.

## Decisions (user, 2026-10-21)

- **The database owns the declaration.** A key's type decides how its values are
  stored, so it is a property of the dataset, like the schema keys themselves,
  which the GUI already reads from the DB. `scistack.toml` was rejected: it
  doesn't move with the `.duckdb` and can disagree with the stored rows.
- **`configure_database(schema_key_types=...)` writes to the DB** instead of being
  a second source:
  - the key has no stored type → it is stored;
  - the same type is stored → nothing happens;
  - a different type is stored → `SchemaKeyTypeConflictError`.
- **Standalone scifor owns its own declaration in the call:**
  `scifor.for_each(..., schema_key_types=...)`. scidb passes the DB's declaration
  down through the same argument.
- **The conversion rule moves to scifor** (`canonical_numeric_value`), and scidb
  imports it. Today `condense_numeric` uses `int()`, which diverges from that rule.
- **GUI checkbox "Numeric"** (unchecked = as stored, checked = declared numeric).
  A declared `"string"` is shown read-only.
- **Q2:** only a key_regex saved through the GUI auto-declares `numeric`, and only
  when the pattern is digit-only and the key is undeclared. A hand-edited TOML
  regex only gets a hint.
- No migration of stored rows (beta policy). Changing a key that already has data
  shows a re-spelling impact first and needs explicit confirmation.

## The three states (unchanged semantics)

| State | Storage | PathInput matching | Load type |
|---|---|---|---|
| undeclared | as spelled | zero-pad bridge → `SchemaKeyTypeError` | inferred per key |
| `"numeric"` | `"001"` → `1` | bridges padding | numbers |
| `"string"` | as spelled | exact only | strings |

## Stage 1: scidb stores the declaration (owner `scidb/schema_types.py`)

- Table `_schema_key_types(key VARCHAR PRIMARY KEY, key_type VARCHAR NOT NULL)`.
  No user identity is recorded.
- `ensure_table(duck)`; `read_types(duck)` returns `{}` when the table is missing
  (old DB opened read-only).
- `impact(db, key, new_type)`:
  - lists the stored values that would change spelling (`'001' → '1'`);
  - lists the locations that would merge (`'1'` and `'001'` both becoming `1`);
  - lists the values that cannot convert (`'r'`);
  - built on the existing `canonical_numeric_value` + `schema_str`.
- `set_type(db, key, new_type | None, *, confirm_respelling=False)` is the writer
  for the GUI and CLI. Unknown key or bad type → `ValueError`. Values that can't
  convert → `SchemaKeyTypeError`. Re-spelling without confirmation →
  `SchemaKeyTypeChangeError` carrying `.impact`.
- `declare(db, types)` handles the constructor kwarg using the rules above. A
  first declaration that would re-spell stored values raises
  `SchemaKeyTypeChangeError` as well.
- `DatabaseManager.dataset_schema_key_types` becomes a read-only **property**
  over a cached dict:
  - loaded at open;
  - re-read on `reopen()`, since another process may have changed it;
  - replaced on `set_type`, which also drops `_schema_key_numeric_cache`.
  - Every consumer keeps working unchanged: foreach, scistackplotdb,
    `+scidb/for_each.m`.
- `DatabaseManager.set_schema_key_type(...)` and `.schema_key_type_impact(...)`
  are thin delegates.
- Logging:
  - the `configure_database` header shows the **stored** types;
  - INFO on every write (key, old → new, counts);
  - DEBUG on a no-op declare.
- Error text that says "declare via configure_database" also names the GUI
  checkbox and `db.set_schema_key_type`.
- Tests: `scidb/tests/test_schema_key_types_stored.py`. Existing tests stay valid.

## Stage 2: one conversion rule in scifor + standalone declaration

- Move `canonical_numeric_value` and `VALID_SCHEMA_KEY_TYPES` to
  `scifor/schema_types.py`, raising a scifor `SchemaKeyTypeError`. scidb's
  `SchemaKeyTypeError` subclasses it, so existing `except` clauses keep working.
- `condense_numeric` uses the rule.
- `scifor.for_each(..., schema_key_types=...)`:
  - `numeric` converts every source;
  - `string` keeps values verbatim and matches paths exactly;
  - keys you don't list keep today's automatic behavior.
- scidb passes its stored types down. MATLAB parity: `+scifor/for_each.m`
  option, and `+scidb/for_each.m` passes it down.

## Stage 3: `scifor.key_regex_is_digit_only(pattern)`

Next to `key_regex_pattern_error`. It walks `re._parser`; anything it doesn't
recognize returns False. Table test.

## Stage 4: GUI backend + CLI

- RPCs: `get_schema_key_types`, `schema_key_type_impact`, `set_schema_key_type`.
- Saving a key_regex through the path_input_service auto-declares `numeric` when
  the pattern is digit-only and the key is undeclared, and logs it.
- `scidb` CLI: an Inspector read and a Mutator write (`mutate.py` checklist).

## Stage 5: Frontend

- A "Numeric" checkbox per key pill in the PathInput panel's Schema Keys section.
- Project-wide tooltip.
- An impact confirmation dialog.
- A digit-only-regex hint.
- Pure helpers tested; both vite targets rebuilt.

## Stage 6: Docs

- Update `docs/claude/schema-key-types.md`.
- New section in docs/gui-manual-testing-todo.md.
