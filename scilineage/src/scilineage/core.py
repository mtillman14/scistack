"""Core lineage tracking system.

This module provides a system for building data processing pipelines
with automatic provenance tracking.

Example:
    @lineage_fcn
    def process_signal(raw: np.ndarray, cal_factor: float) -> np.ndarray:
        return raw * cal_factor

    result = process_signal(raw_data, 2.5)  # Returns LineageFcnResult
    print(result.data)  # The actual computed data
    print(result.invoked.inputs)  # Captured inputs for lineage

For multi-output functions:
    @lineage_fcn(unpack_output=True)
    def split(data):
        return data[:len(data)//2], data[len(data)//2:]

    first, second = split(data)  # Each is a separate LineageFcnResult
"""

import inspect
import logging
from functools import wraps
from hashlib import sha256
from typing import Any, Callable

logger = logging.getLogger(__name__)

from .backend import _get_backend
from .hashing import compute_function_hash
from .inputs import classify_inputs, is_trackable_variable

STRING_REPR_DELIMITER = "-"


# -----------------------------------------------------------------------------
# Core Classes
# -----------------------------------------------------------------------------


class LineageFcn:
    """
    Wraps a function to enable lineage tracking.

    When called, creates a LineageFcnInvocation that tracks inputs.
    The function's bytecode is hashed for reproducibility checking.

    Attributes:
        fcn: The wrapped function
        unpack_output: Whether to unpack tuple returns into separate LineageFcnResults
        unwrap: Whether to unwrap special input types to raw data
        hash: SHA-256 hash of function bytecode + unpack_output
        invocations: All LineageFcnInvocations created from this LineageFcn
    """

    def __init__(self, fcn: Callable, unpack_output: bool = False, unwrap: bool = True, generates_file: bool = False):
        """
        Initialize a LineageFcn wrapper of a function.

        Args:
            fcn: The function to wrap
            unpack_output: If True, unpack tuple returns into separate LineageFcnResults.
                          If False (default), the return value is wrapped as a single
                          LineageFcnResult (even if it's a tuple).
            unwrap: If True (default), unwrap LineageFcnResult inputs to their raw
                   data before calling the function. If False, pass the wrapper
                   objects directly (useful for debugging/inspection).
            generates_file: If True, this function produces side-effect files
                           rather than returning data. Not included in hash.
        """
        self.fcn = fcn
        self.unpack_output = unpack_output
        self.unwrap = unwrap
        self.generates_file = generates_file
        self.invocations: tuple[LineageFcnInvocation, ...] = ()

        # Hash function bytecode + constants for reproducibility.
        # Use shared compute_function_hash (bytecode-based, ignores formatting/comments).
        # Get full hash (not truncated) so we have maximum entropy.
        fcn_hash = compute_function_hash(fcn, truncate=64)

        # Include unpack_output in hash to distinguish function configurations
        string_repr = f"{fcn_hash}{STRING_REPR_DELIMITER}{unpack_output}"
        self.hash = sha256(string_repr.encode()).hexdigest()
        logger.debug("LineageFcn created: %s hash=%s", fcn.__name__, self.hash[:12])

    def __repr__(self) -> str:
        return f"LineageFcn(fcn={self.fcn.__name__}, unpack_output={self.unpack_output}, unwrap={self.unwrap})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LineageFcn):
            return False
        return self.hash == other.hash

    def __hash__(self) -> int:
        return int(self.hash[:16], 16)

    def __call__(self, *args, **kwargs) -> "LineageFcnResult | tuple[LineageFcnResult, ...]":
        """
        Create a LineageFcnInvocation and execute.

        Returns:
            LineageFcnResult or tuple of LineageFcnResults wrapping the result(s)
        """
        invocation = LineageFcnInvocation(self, *args, **kwargs)

        # Check for cached results
        _backend = _get_backend()
        if _backend is not None:
            logger.debug("cache lookup for %s", self.fcn.__name__)
            try:
                cached = _backend.find_by_lineage(invocation)
                if cached is not None:
                    logger.debug("cache HIT for %s — %d outputs", self.fcn.__name__, len(cached))
                    # Wrap cached values in LineageFcnResult
                    outputs = tuple(
                        LineageFcnResult(invocation, i, True, val)
                        for i, val in enumerate(cached)
                    )
                    invocation.outputs = outputs
                    if len(outputs) == 1:
                        return outputs[0]
                    return outputs
            except Exception:
                logger.debug("cache miss for %s, executing", self.fcn.__name__)

        return invocation(*args, **kwargs)


class LineageFcnInvocation:
    """
    Represents a specific invocation of a LineageFcn with captured inputs.

    Tracks:
    - The parent LineageFcn (function definition)
    - All input arguments (positional and keyword)
    - Output(s) after execution

    Attributes:
        fcn: The parent LineageFcn that created this
        inputs: Dict mapping argument names to values
        outputs: Tuple of LineageFcnResults after execution
        unwrap: Whether to unwrap inputs before calling the function
    """

    def __init__(self, fcn: LineageFcn, *args, **kwargs):
        """
        Initialize a LineageFcnInvocation.

        Args:
            fcn: The parent LineageFcn
            *args: Positional arguments passed to the function
            **kwargs: Keyword arguments passed to the function
        """
        self.fcn = fcn
        self.unwrap = fcn.unwrap
        self.inputs: dict[str, Any] = {}

        # Bind args to their proper parameter names using the function signature.
        # Does NOT apply defaults — only explicitly-passed values are captured,
        # consistent with for_each() which records only what the caller supplied.
        # Falls back to arg_N naming if binding fails (shouldn't happen for normal
        # functions, but guards against edge cases like C-extension callables).
        try:
            sig = inspect.signature(fcn.fcn)
            bound = sig.bind(*args, **kwargs)
            self.inputs = dict(bound.arguments)
        except (TypeError, ValueError):
            for i, arg in enumerate(args):
                self.inputs[f"arg_{i}"] = arg
            self.inputs.update(kwargs)

        self.outputs: tuple[LineageFcnResult, ...] = ()

    @property
    def hash(self) -> str:
        """Dynamic hash based on fcn + inputs (lineage-based, metadata-agnostic)."""
        return self.compute_lineage_hash()

    def __hash__(self) -> int:
        return int(self.hash[:16], 16)

    def __repr__(self) -> str:
        return (
            f"LineageFcnInvocation(fcn={self.fcn.fcn.__name__}, "
            f"n_inputs={len(self.inputs)}, unpack_output={self.fcn.unpack_output})"
        )

    @property
    def is_complete(self) -> bool:
        """True if all inputs are concrete values (not pending invocations)."""
        for value in self.inputs.values():
            if isinstance(value, LineageFcnResult) and not value.is_complete:
                return False
        return True

    def __call__(self, *args, **kwargs) -> "LineageFcnResult | tuple[LineageFcnResult, ...]":
        """
        Execute the function if complete, return LineageFcnResult(s).

        Returns:
            LineageFcnResult or tuple of LineageFcnResults wrapping the result(s)
        """
        if self.is_complete:
            logger.debug("executing %s with %d args (unwrap=%s)",
                          self.fcn.fcn.__name__, len(args), self.unwrap)
            # Resolve arguments - unwrap if self.unwrap is True
            resolved_args = []
            for arg in args:
                if self.unwrap:
                    resolved_args.append(self._deep_unwrap(arg))
                else:
                    resolved_args.append(arg)

            resolved_kwargs = {}
            for k, v in kwargs.items():
                if self.unwrap:
                    resolved_kwargs[k] = self._deep_unwrap(v)
                else:
                    resolved_kwargs[k] = v

            result = self.fcn.fcn(*resolved_args, **resolved_kwargs)

            # Handle output based on unpack_output setting
            if self.fcn.unpack_output:
                # Unpack tuple into separate LineageFcnResults
                if not isinstance(result, tuple):
                    raise ValueError(
                        f"Function {self.fcn.fcn.__name__} has unpack_output=True "
                        f"but did not return a tuple."
                    )
                outputs = tuple(
                    LineageFcnResult(self, i, True, val) for i, val in enumerate(result)
                )
            else:
                # Wrap entire result as single LineageFcnResult
                outputs = (LineageFcnResult(self, 0, True, result),)
        else:
            # Not complete - create placeholder output(s)
            outputs = (LineageFcnResult(self, 0, False, None),)

        self.outputs = outputs
        logger.debug("executed %s -> %d outputs (unpack=%s)",
                      self.fcn.fcn.__name__, len(outputs), self.fcn.unpack_output)

        if len(outputs) == 1:
            return outputs[0]
        return outputs

    def _deep_unwrap(self, value: Any) -> Any:
        """Recursively unwrap LineageFcnResults and trackable variables to raw data.

        Handles cases like:
        - LineageFcnResult -> raw data
        - BaseVariable -> raw data
        - BaseVariable wrapping LineageFcnResult -> raw data (recursive)
        """
        # Unwrap LineageFcnResult
        if isinstance(value, LineageFcnResult):
            return value.data

        # Unwrap trackable variable (e.g., BaseVariable)
        if is_trackable_variable(value):
            inner = getattr(value, "data", value)
            # If the variable's data is itself a LineageFcnResult, unwrap that too
            if isinstance(inner, LineageFcnResult):
                return inner.data
            return inner

        return value

    def compute_lineage_hash(self) -> str:
        """
        Compute a hash representing this computation's lineage.

        The hash is based on:
        - Function hash (bytecode + constants)
        - For LineageFcnResult inputs: use lineage-based hash
        - For trackable variable inputs: use lineage_hash if computed, else content+type
        - For raw values: use content hash

        Returns:
            SHA-256 hash encoding the computation lineage
        """
        classified = classify_inputs(self.inputs)
        input_tuples = [c.to_cache_tuple() for c in classified]
        hash_input = f"{self.fcn.hash}{STRING_REPR_DELIMITER}{input_tuples}"
        result_hash = sha256(hash_input.encode()).hexdigest()
        logger.debug("lineage hash for %s: %s (%d classified inputs)",
                      self.fcn.fcn.__name__, result_hash[:12], len(classified))
        return result_hash

    def _matches(self, other: "LineageFcnInvocation") -> bool:
        """Check if this is equivalent to another LineageFcnInvocation."""
        if self.fcn.hash != other.fcn.hash:
            return False

        # Check if inputs match
        if set(self.inputs.keys()) != set(other.inputs.keys()):
            return False

        for key in self.inputs:
            self_val = self.inputs[key]
            other_val = other.inputs[key]

            # Compare LineageFcnResults by hash
            if isinstance(self_val, LineageFcnResult) and isinstance(other_val, LineageFcnResult):
                if self_val.hash != other_val.hash:
                    return False
            elif isinstance(self_val, LineageFcnResult) or isinstance(other_val, LineageFcnResult):
                return False
            else:
                # Compare other values directly
                try:
                    if self_val != other_val:
                        return False
                except (ValueError, TypeError):
                    # numpy arrays raise ValueError for == comparison
                    try:
                        import numpy as np

                        if isinstance(self_val, np.ndarray) and isinstance(
                            other_val, np.ndarray
                        ):
                            if not np.array_equal(self_val, other_val):
                                return False
                        else:
                            return False
                    except ImportError:
                        return False

        return True


class LineageFcnResult:
    """
    Wraps a function output with lineage information.

    Contains:
    - Reference to the LineageFcnInvocation that produced it
    - Output index (for multi-output functions)
    - The actual computed data

    This is the key to provenance: every LineageFcnResult knows its parent
    LineageFcnInvocation, which knows its inputs (possibly other LineageFcnResults).

    Attributes:
        invoked: The LineageFcnInvocation that produced this output
        output_num: Index of this output (0-based)
        is_complete: True if the data has been computed
        data: The actual computed data (None if not complete)
        hash: SHA-256 hash encoding the full lineage
    """

    def __init__(
        self,
        invoked: LineageFcnInvocation,
        output_num: int,
        is_complete: bool,
        data: Any,
    ):
        """
        Initialize a LineageFcnResult.

        Args:
            invoked: The LineageFcnInvocation that produced this
            output_num: Index of this output
            is_complete: Whether the data has been computed
            data: The computed data (or None if not complete)
        """
        self.invoked = invoked
        self.output_num = output_num
        self.is_complete = is_complete
        self.data = data if is_complete else None

        string_repr = (
            f"{invoked.hash}{STRING_REPR_DELIMITER}"
            f"output{STRING_REPR_DELIMITER}{output_num}"
        )
        self.hash = sha256(string_repr.encode()).hexdigest()

    def __repr__(self) -> str:
        fcn_name = self.invoked.fcn.fcn.__name__
        return f"LineageFcnResult(fn={fcn_name}, output={self.output_num}, complete={self.is_complete})"

    def __str__(self) -> str:
        """Show only the data when printed."""
        return str(self.data)

    def __eq__(self, other: object) -> bool:
        """Compare by hash if LineageFcnResult, otherwise compare data."""
        if isinstance(other, LineageFcnResult):
            return self.hash == other.hash
        return self.data == other

    def __hash__(self) -> int:
        return int(self.hash[:16], 16)


# -----------------------------------------------------------------------------
# Decorator
# -----------------------------------------------------------------------------


def lineage_fcn(
    func: Callable | None = None,
    *,
    unpack_output: bool = False,
    unwrap: bool = True,
    generates_file: bool = False,
) -> "Callable[[Callable], LineageFcn] | LineageFcn":
    """
    Decorator to convert a function into a LineageFcn for lineage tracking.

    Can be used with or without parentheses:

        @lineage_fcn
        def process_signal(raw, calibration):
            return raw * calibration

        @lineage_fcn()
        def another_function(x):
            return x * 2

    For multi-output functions, use unpack_output=True:

        @lineage_fcn(unpack_output=True)
        def split(data):
            return data[:len(data)//2], data[len(data)//2:]

        first_half, second_half = split(my_data)  # Each is a LineageFcnResult

    For debugging/inspection, use unwrap=False to receive the full objects:

        @lineage_fcn(unwrap=False)
        def debug_process(var):
            print(f"Input record_id: {var.record_id}")
            print(f"Input metadata: {var.metadata}")
            return var.data * 2

    Args:
        func: The function to wrap (when used without parentheses)
        unpack_output: If True, unpack tuple returns into separate LineageFcnResults.
                      If False (default), the return value is wrapped as a single
                      LineageFcnResult (even if it's a tuple).
        unwrap: If True (default), unwrap LineageFcnResult and variable inputs
               to their raw data (.data). If False, pass the wrapper
               objects directly for inspection/debugging.
        generates_file: If True, this function produces side-effect files
                       rather than returning data. Enables lineage-only
                       save and cache-hit behavior without storing data.

    Returns:
        A LineageFcn wrapping the function, or a decorator that creates one
    """

    def decorator(fcn: Callable) -> LineageFcn:
        t = LineageFcn(fcn, unpack_output, unwrap, generates_file)
        return wraps(fcn)(t)

    if func is not None:
        # Called without parentheses: @lineage_fcn
        return decorator(func)
    # Called with parentheses: @lineage_fcn() or @lineage_fcn(unpack_output=True)
    return decorator


# -----------------------------------------------------------------------------
# Manual intervention
# -----------------------------------------------------------------------------


def _create_manual_sentinel() -> LineageFcn:
    """Create a named sentinel LineageFcn for manual interventions."""
    def manual():
        """Placeholder for manual data interventions."""
        pass
    return LineageFcn(manual)


_MANUAL_FCN: LineageFcn = _create_manual_sentinel()


def manual(data: Any, label: str, reason: str = "") -> LineageFcnResult:
    """
    Wrap manually edited data in a LineageFcnResult, preserving pipeline lineage.

    Use this when you step outside the tracked pipeline to make a manual
    correction, then want to re-enter it. The label and reason are recorded
    in the lineage graph so the intervention is documented and auditable.

    Args:
        data:   The manually edited data to re-enter the pipeline.
        label:  Short identifier for this intervention (e.g. "outlier_removal").
                Shows up in lineage records.
        reason: Human-readable explanation of why the edit was made.
                Stored in the lineage graph for future reference.

    Returns:
        LineageFcnResult carrying the data and a lineage record marking it as
        a manual intervention.

    Example:
        result = bandpass_filter(raw_emg)

        # Step out — manual correction
        edited = result.data.copy()
        edited = edited[edited["amplitude"] > 0.1]

        # Re-enter — manual step is now documented in lineage
        corrected = manual(
            edited,
            label="outlier_removal",
            reason="amplitude < 0.1 in trial 3 is sensor artifact",
        )
        MaxActivation.save(compute_max(corrected), subject=1, session="A")
    """
    # Store label, reason, and data as inputs so the lineage hash depends on
    # both the annotation and the actual content of the data.
    invocation = LineageFcnInvocation(_MANUAL_FCN, label=label, reason=reason, data=data)
    output = LineageFcnResult(invocation, 0, True, data)
    invocation.outputs = (output,)
    return output


def make_tuple_unpacking_wrapper(lineage_fn: Callable) -> Callable:
    """Wrap a LineageFcn to unpack tuple results into separate LineageFcnResults.

    When a @lineage_fcn function returns a tuple (e.g., for multiple outputs),
    this wrapper unpacks the tuple so that each output gets its own
    LineageFcnResult with just its element, rather than all outputs sharing
    the same LineageFcnResult containing the full tuple.

    This is useful when integrating with iteration frameworks (like scifor)
    that expect tuple-returning functions to produce separate values per output.

    Args:
        lineage_fn: A LineageFcn or any callable that may return LineageFcnResult

    Returns:
        A wrapped function that unpacks tuple-wrapped LineageFcnResults

    Example:
        @lineage_fcn
        def split_data(x):
            return x[:10], x[10:]

        wrapped = make_tuple_unpacking_wrapper(split_data)
        first, second = wrapped(data)  # Each is its own LineageFcnResult
    """
    from .lineage import get_raw_value

    def wrapped(*args, **kwargs):
        result = lineage_fn(*args, **kwargs)

        # If result is a LineageFcnResult wrapping a tuple, unpack it
        # This allows functions that return tuples to work with multiple outputs
        # even without unpack_output=True
        if isinstance(result, LineageFcnResult):
            raw = get_raw_value(result)
            if isinstance(raw, tuple):
                # Unpack tuple - return tuple of LineageFcnResults (one per element)
                # Each keeps the same lineage but different data
                unpacked = []
                for i, elem in enumerate(raw):
                    # Create a new LineageFcnResult for each tuple element
                    unpacked_result = type(result)(
                        result.invoked,
                        i,  # output_num
                        result.is_complete,
                        elem  # just this element, not the tuple
                    )
                    unpacked.append(unpacked_result)
                return tuple(unpacked)

        return result

    wrapped.__name__ = getattr(lineage_fn, "__name__", "lineage_fcn")

    # Store the original function's parameter names so scidb can check which
    # metadata keys to inject for generates_file functions
    try:
        original_fn = lineage_fn.fcn if hasattr(lineage_fn, 'fcn') else lineage_fn
        sig = inspect.signature(original_fn)
        wrapped.__scidb_params__ = set(sig.parameters.keys())
    except (ValueError, TypeError):
        wrapped.__scidb_params__ = set()

    # Mark as a lineage wrapper so scidb can detect and reconstruct BaseVariable inputs.
    # Only set True when wrapping an actual LineageFcn (has .fcn attribute), NOT for
    # plain functions — otherwise _reconstruct_variable_inputs runs on non-variable
    # inputs and causes TypeError on RawSignal comparisons.
    wrapped.__lineage_wrapper__ = hasattr(lineage_fn, 'fcn')

    # Propagate .fcn so compute_function_hash can unwrap through any number of
    # wrapper layers to reach the original user function.  Without this,
    # double-wrapping (scihist wraps, then scidb wraps again) causes
    # compute_function_hash to hash the wrapper bytecode instead of the
    # original function, producing mismatched __fn_hash values.
    if hasattr(lineage_fn, 'fcn'):
        wrapped.fcn = lineage_fn.fcn

    return wrapped
