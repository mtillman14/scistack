# Function Hash Unification Analysis

## Current State: Two Different Hashing Methods

### Method 1: Source-based (scidb)
**Location:** `scidb/src/scidb/foreach_config.py:9-21`
```python
def _compute_fn_hash(fn: Callable) -> str:
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        src = getattr(fn, "__name__", repr(fn))
    return hashlib.sha256(src.encode()).hexdigest()[:16]
```

### Method 2: Bytecode-based (scilineage)
**Location:** `scilineage/src/scilineage/core.py:78-86`
```python
fcn_code = fcn.__code__.co_code
fcn_consts = str(fcn.__code__.co_consts).encode()
combined_code = fcn_code + fcn_consts
fcn_hash = sha256(combined_code).hexdigest()

string_repr = f"{fcn_hash}{STRING_REPR_DELIMITER}{unpack_output}"
self.hash = sha256(string_repr.encode()).hexdigest()
```

## Use Case Analysis

The function hash is used for **staleness detection** in two scenarios:

### 1. Lineage-based check (`scihist.for_each` outputs)
- Uses `LineageFcn.hash` from `_lineage.function_hash`
- Compares stored hash vs current function's hash
- If mismatch → output is stale, needs recomputation
- Source: `scihist-lib/src/scihist/state.py:139`

### 2. Version keys fallback (`scidb.for_each` outputs)
- Uses `__fn_hash` from `version_keys`
- Computed via `_compute_fn_hash(fn)`
- If mismatch → output is stale
- Source: `scihist-lib/src/scihist/state.py:266`

## Comparison

| Aspect | Source-based | Bytecode-based | Winner |
|--------|-------------|----------------|---------|
| **Detects logic changes** | ✅ Yes | ✅ Yes | Tie |
| **Ignores formatting** | ❌ No (too sensitive) | ✅ Yes | **Bytecode** |
| **Ignores comments** | ❌ No (too sensitive) | ✅ Yes | **Bytecode** |
| **Ignores whitespace** | ❌ No (too sensitive) | ✅ Yes | **Bytecode** |
| **Works with built-ins** | ⚠️ Fallback to `__name__` | ⚠️ N/A (no bytecode) | Tie |
| **Debuggability** | ✅ Easy (can read source) | ⚠️ Harder (bytecode) | Source |
| **Stability** | ❌ Changes often | ✅ Stable | **Bytecode** |
| **Includes constants** | ✅ Yes (in source) | ✅ Yes (explicit) | Tie |
| **Hash length** | 16 chars | 64 chars | Source (shorter) |

## Real-World Impact Examples

### Scenario 1: Code reformatting (Black, autopep8)
```python
# Before
def bandpass(signal,low,high):
    return butter(signal, [low,high])

# After (formatted)
def bandpass(signal, low, high):
    return butter(signal, [low, high])
```
- **Source-based:** Hash changes ❌ → invalidates all cached outputs
- **Bytecode-based:** Hash unchanged ✅ → cached outputs still valid

### Scenario 2: Adding documentation
```python
# Before
def bandpass(signal, low, high):
    return butter(signal, [low, high])

# After (documented)
def bandpass(signal, low, high):
    """Apply bandpass filter."""
    return butter(signal, [low, high])
```
- **Source-based:** Hash changes ❌ → invalidates cache
- **Bytecode-based:** Hash unchanged ✅ → cache valid

### Scenario 3: Actual logic change
```python
# Before
def bandpass(signal, low, high):
    return butter(signal, [low, high])

# After (bug fix)
def bandpass(signal, low, high):
    return butter(signal, [low, high], btype='bandpass')  # Added argument
```
- **Source-based:** Hash changes ✅
- **Bytecode-based:** Hash changes ✅

## Recommendation: Use Bytecode-based

**Reasons:**
1. **Scientific correctness**: Reformatting code should NOT invalidate scientific results
2. **Developer experience**: Code linting, formatting, documentation shouldn't trigger expensive recomputation
3. **Stability**: Bytecode changes only when logic changes
4. **Current best practice**: scilineage already uses this method successfully

**Implementation:**
1. Replace `_compute_fn_hash()` in scidb with bytecode-based approach
2. Keep hash length at 16 chars (truncate) to maintain compatibility
3. Update tests to expect bytecode-based hashes

## Edge Cases to Handle

### 1. Functions without `__code__` (built-ins, C extensions)
```python
def _compute_fn_hash(fn: Callable) -> str:
    try:
        fcn_code = fn.__code__.co_code
        fcn_consts = str(fn.__code__.co_consts).encode()
        combined_code = fcn_code + fcn_consts
        return hashlib.sha256(combined_code).hexdigest()[:16]
    except AttributeError:
        # Fallback for built-ins, C extensions
        name = getattr(fn, "__name__", repr(fn))
        return hashlib.sha256(name.encode()).hexdigest()[:16]
```

### 2. LineageFcn wrappers
- For `LineageFcn` instances, extract `fn.fcn.__code__`
- Already handled in `state.py:265` via `fn.fcn if hasattr(fn, "fcn") else fn`

### 3. Hash length consistency
- scidb currently uses 16 chars (truncated)
- scilineage uses full 64 chars
- **Decision:** Standardize on 16 chars (sufficient entropy, saves space)

## Migration Path

1. Update `_compute_fn_hash()` to use bytecode
2. Keep both hashes temporarily for validation
3. Run tests to verify they produce stable results
4. Document that reformatting won't invalidate cache (feature, not bug!)

## Open Question: Should we update scilineage too?

scilineage also includes `unpack_output` in its hash:
```python
string_repr = f"{fcn_hash}{STRING_REPR_DELIMITER}{unpack_output}"
self.hash = sha256(string_repr.encode()).hexdigest()
```

**Question:** Should scidb also hash configuration flags (like `distribute`, `as_table`)?

**Answer:** No - those are already in `version_keys` separately and affect call_id. The function hash should ONLY reflect the function body.
