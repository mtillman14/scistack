"""MATLAB bridge for SciStack.

Runs scidb's ``for_each`` on behalf of MATLAB and provides
:class:`MatlabFunctionIdentity`, a lightweight MATLAB function handle for
``scidb.check_node_state`` node coloring.

Usage from MATLAB (via py. interface):
    py.scimatlab.bridge.MatlabFunctionIdentity(source_hash, 'my_func', false)
"""

from .bridge import (
    MatlabFunctionIdentity,
    get_surrogate_class,
    register_matlab_variable,
)

__all__ = [
    "MatlabFunctionIdentity",
    "register_matlab_variable",
    "get_surrogate_class",
]
