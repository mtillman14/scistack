"""MATLAB bridge for SciStack.

Provides proxy classes that satisfy the duck-typing contracts of
thunk-lib's Thunk/PipelineThunk/ThunkOutput, allowing MATLAB functions
to participate in the SciStack lineage system without modifying any
existing Python packages.

Usage from MATLAB (via py. interface):
    py.sci_matlab.bridge.MatlabThunk(source_hash, 'my_func', false)
"""

from .bridge import (
    MatlabThunk,
    MatlabPipelineThunk,
    check_cache,
    make_thunk_output,
    register_matlab_variable,
    get_surrogate_class,
)

__all__ = [
    "MatlabThunk",
    "MatlabPipelineThunk",
    "check_cache",
    "make_thunk_output",
    "register_matlab_variable",
    "get_surrogate_class",
]
