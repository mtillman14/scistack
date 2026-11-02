# Contributing & Dev Setup

<!-- Ground truth (source win over prose). Verified against:
     repo layout: each package in its own folder with src/<pkg>/ + tests/;
     dev-install.sh (editable install in dependency order — the one owner of
     that order); .github/scripts/test-shards.sh (one pytest process per package);
     declared [dependencies] giving the layer order; CLAUDE.md (fixes belong in the
     matching scistack layer; add logging + regression tests when fixing). -->

SciStack is a monorepo of small, single-responsibility packages. This page covers
getting a development environment running and the conventions for changes.

## Repository layout

Each package is its own folder with a `src/<package>/` module and a `tests/`
suite (`scistack-gui` keeps its module at `scistack_gui/`):

```
general-sqlite-database/
  scistacklog/   scicanonicalhash/   scifor/   sciduckdb/   scidb/   scimatlab/
  scistackplot/  scistackplotdb/     scistackstats/   scistackstatsdb/
  scistack/      scistack-gui/       docs/     mkdocs.yml   dev-install.sh
```

The dependency direction (and therefore install/build order) is
`scidb → {scifor, sciduckdb, scicanonicalhash, scistacklog}`, with
`scimatlab`, `scistackplotdb`, `scistackstatsdb` and `scistack-gui` on top of
`scidb`. See [Architecture & Layers](../concepts/architecture.md) for the
user-facing layers.

## Set up a dev environment

Install every package in **editable** mode, in dependency order, so changes in one
package are picked up by the others without reinstalling:

```bash
git clone https://github.com/mtillman14/general-sqlite-database
cd general-sqlite-database
bash dev-install.sh
```

## Run the tests

The test suites are the **source of truth** for behavior — when documentation or
intuition disagrees with a test, the test wins. Run **one package per pytest
invocation**: the packages' `conftest.py` modules share a name and collide when
two `tests/` folders are collected together.

```bash
python -m pytest scidb/tests -x -q
python -m pytest scistack-gui/tests -x -q
```

`scimatlab`'s MATLAB suite needs a MATLAB licence; CI skips it.

## Conventions

A few project-specific norms (from `CLAUDE.md`):

- **Fix things in the layer that owns them.** A storage bug belongs in `scidb` /
  `sciduckdb`, an iteration bug in `scifor`, a hashing bug in `scicanonicalhash`, and
  so on — not patched a layer up. Only genuinely GUI-specific issues live in the
  GUI layer.
- **When fixing a bug, add observability and a regression test.** Prefer adding
  logging (or timing) around the affected internals, and a test that pins the
  corrected behavior so it can't silently regress.
- **Keep changes consistent with the surrounding code** — match the existing
  naming, structure, and idioms of the package you're editing.

## Documentation changes

Docs live in `docs/` and are built with MkDocs. The prose is reconciled against
the package tests, so when you change behavior, update the affected page(s) and
verify with a strict build — see [Building These Docs](building-docs.md).

**Next:** [Building These Docs](building-docs.md) ·
[Architecture & Layers](../concepts/architecture.md) ·
[Installation](../getting-started/installation.md)
