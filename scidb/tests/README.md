# SciStack Test Suite

Comprehensive pytest-compatible test suite for scidb implementation.

## Test Structure

```
tests/
├── conftest.py          # Shared fixtures and sample variable classes
├── test_exceptions.py   # Tests for custom exceptions
├── test_hashing.py      # Tests for canonical_hash() and generate_record_id()
├── test_storage.py      # Tests for DataFrame serialization
├── test_variable.py     # Tests for BaseVariable ABC
├── test_database.py     # Tests for DatabaseManager
├── test_init.py         # Tests for public API exports
├── test_integration.py  # End-to-end integration tests
├── test_thunk.py        # Tests for thunk system (Phase 2)
├── test_lineage.py      # Tests for lineage tracking (Phase 2)
└── requirements.txt     # Test dependencies
```

## Running Tests

### Install dependencies

```bash
pip install -e ".[dev]"
# or
pip install pytest pytest-cov numpy pandas
```

### Run all tests

```bash
pytest
```

### Run with verbose output

```bash
pytest -v
```

### Run specific test file

```bash
pytest tests/test_hashing.py
```

### Run specific test class

```bash
pytest tests/test_database.py::TestSave
```

### Run specific test

```bash
pytest tests/test_database.py::TestSave::test_save_returns_record_id
```

### Run with coverage

```bash
pytest --cov=src/scidb --cov-report=html
```

### Run only fast tests (skip integration)

```bash
pytest --ignore=tests/test_integration.py
```

## Test Categories

### Phase 1: Core Infrastructure

- `test_exceptions.py` - Exception hierarchy and behavior
- `test_hashing.py` - Deterministic hashing for various data types
- `test_storage.py` - DataFrame serialization/deserialization
- `test_variable.py` - BaseVariable ABC behavior
- `test_database.py` - DatabaseManager operations
- `test_init.py` - Public API exports

### Phase 2: Thunk System & Lineage

- `test_thunk.py` - Thunk decorator, PipelineThunk, OutputThunk
- `test_lineage.py` - Lineage extraction and provenance tracking

### Phase 3: Computation Caching

- `test_caching.py` - Cache population, lookup, invalidation, and stats

### Integration Tests

- `test_integration.py` - Full end-to-end workflows

## Fixtures

The `conftest.py` provides these fixtures:

| Fixture | Description |
|---------|-------------|
| `temp_db_path` | Temporary database file path |
| `db` | Fresh DatabaseManager instance |
| `configured_db` | Global database configured via configure_database() |
| `scalar_class` | Sample BaseVariable for scalar values |
| `array_class` | Sample BaseVariable for 1D numpy arrays |
| `matrix_class` | Sample BaseVariable for 2D numpy arrays |
| `dataframe_class` | Sample BaseVariable for pandas DataFrames |

## Expected Test Count

Approximately 150+ tests covering:

- Exception hierarchy and messages
- Hashing primitives, collections, numpy, pandas
- DataFrame serialization round-trips
- BaseVariable ABC enforcement
- Table name generation
- Reserved metadata key validation
- Database registration and table creation
- Save/load operations
- Version history
- Idempotent saves
- Multiple variable types
- Database persistence across reconnections
- Error handling
