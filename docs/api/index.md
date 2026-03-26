# API Reference

SciStack's public API spans three packages that work together. The same concepts and nearly identical method names are available in both Python and MATLAB.

## Package Overview

| Package | Purpose | Import |
|---------|---------|--------|
| `scidb` | Core: variables, database, lineage | `from scidb import ...` / `scidb.*` |
| `scirun` | Batch processing (`for_each`) | `from scidb import for_each` / `scidb.for_each(...)` |
| `sci-matlab` | MATLAB wrapper | `addpath(...)` then `scidb.*` |

## API Sections

### [Variables (BaseVariable)](variables.md)

The central abstraction. Every piece of data you store is a subclass of `BaseVariable`.

Key methods: `save()`, `load()`, `load_all()`, `list_versions()`, `save_from_dataframe()`

### [Database](database.md)

Database configuration and management operations.

Key functions: `configure_database()`, `get_database()`, `get_provenance()`, variable groups

### [Lineage (Thunk System)](lineage.md)

Automatic provenance tracking via `@thunk` and caching.

Key items: `@thunk`, `Thunk`, `ThunkOutput`, `generates_file`

### [Batch Processing (for_each)](for-each.md)

Run a function over every combination of metadata values.

Key items: `for_each()`, `Fixed`, `ColumnSelection`, `Merge`, `PathInput`

## Language Notes

All API pages show Python and MATLAB syntax side-by-side using tabs. Click the tab to switch.

**Key differences between languages:**

| Concept | Python | MATLAB |
|---------|--------|--------|
| Variable types | `class MyVar(BaseVariable): pass` | `classdef MyVar < scidb.BaseVariable; end` |
| Save | `MyVar.save(data, subject=1)` | `MyVar().save(data, subject=1)` |
| Load result type | `BaseVariable` instance | `scidb.ThunkOutput` (same `.data`, `.metadata`) |
| Thunk decorator | `@thunk` or `Thunk(fn)` | `scidb.Thunk(@fn)` |
| Column selection | `MyVar["col"]` | `MyVar("col")` (constructor argument) |
| for_each inputs | `dict` | `struct` (field order = argument order) |
| for_each outputs | `list` of classes | cell array of instances |
| Thunk multi-output | Returns tuple | Function must return cell array |
