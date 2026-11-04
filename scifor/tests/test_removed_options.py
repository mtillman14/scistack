"""A removed for_each option must fail loudly.

for_each takes unknown keywords as iteration keys, so without this check a
stale call would quietly iterate over the old option instead of erroring.
"""

import pytest

from scifor.options import reject_removed_options
from scifor.outputs import discard_db_only_kwargs


def test_removed_option_is_refused():
    with pytest.raises(TypeError, match="no longer takes finalized="):
        reject_removed_options({"subject": [1], "finalized": True})


def test_iteration_keys_pass():
    reject_removed_options({"subject": [1], "trial": []})


def test_scifor_for_each_path_refuses_it():
    with pytest.raises(TypeError, match="no longer takes finalized="):
        discard_db_only_kwargs({"finalized": True})
