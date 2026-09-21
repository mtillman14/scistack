"""
Function role — the four-way classification of a pipeline function by its
name prefix (``scidb.roles.function_role``).

One classifier for the whole stack: the prefixes already drive real execution
behaviour inside scidb (endpoint policy, draft/record mode, artifact stamping),
so the GUI must never hold a second copy of the strings. See
``docs/claude/free-code-glue-nodes.md`` §0.

The spelling test that matters: ``stat_`` is **singular**. ``stats_summary``
is an ordinary process function, whatever the sidebar label says.
"""

import pytest

from scidb import FUNCTION_ROLES, function_role
from scidb.roles import ROLE_PREFIX, endpoint_kind


class TestClassification:
    @pytest.mark.parametrize(
        "name,role",
        [
            ("process_emg", "process"),
            ("analyze", "process"),
            ("plot_gait", "plot"),
            ("stat_summary", "stat"),
            ("glue_drop_baseline", "glue"),
        ],
    )
    def test_prefixes(self, name, role):
        assert function_role(name) == role

    def test_stats_plural_is_a_process_function(self):
        # The confusion that prompted naming the concept: the label is
        # "Stats", the prefix is stat_.
        assert function_role("stats_summary") == "process"

    def test_plots_plural_is_a_process_function(self):
        assert function_role("plots_overview") == "process"

    def test_a_bare_prefix_with_nothing_after_it_still_classifies(self):
        assert function_role("glue_") == "glue"

    def test_prefix_must_be_at_the_start(self):
        assert function_role("make_plot_gait") == "process"

    def test_every_role_is_reachable(self):
        assert set(FUNCTION_ROLES) == {"process", "plot", "stat", "glue"}
        produced = {function_role(f"{p}x") for p in ROLE_PREFIX.values()}
        assert produced | {"process"} == set(FUNCTION_ROLES)


class TestEndpointKindStillAgrees:
    """The refactor must not change what scidb treats as an endpoint."""

    @pytest.mark.parametrize(
        "name,kind",
        [
            ("plot_gait", "plot"),
            ("stat_summary", "stat"),
            ("glue_drop_baseline", None),
            ("analyze", None),
            ("stats_summary", None),
        ],
    )
    def test_endpoint_kind(self, name, kind):
        assert endpoint_kind(name) == kind
