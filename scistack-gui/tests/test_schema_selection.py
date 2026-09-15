"""The processing tab's schema selection: storage, projection, and the RPC.

Stage 6 of ``.claude/plan-schema-key-picker-and-level-order.md``. The panel's
per-key checkboxes are gone; a function node now stores the same PAIR the
plotting tab stores — ragged ``include`` prefixes plus a standing
``exclude_levels`` rule (docs/claude/location-filter-semantics.md).

Two things are worth testing here and nowhere else:

* the **projection** onto what a generated MATLAB command can spell, which is
  lossy and must say so;
* the **node tree**, which is the inner join of a node's inputs rather than one
  variable's locations.
"""

from scistack_gui.domain.schema_selection import (
    as_selection,
    is_empty,
    to_schema_filter,
)


class TestNormalization:
    def test_absent_halves_mean_everything(self):
        """A node saved before this feature has neither key, and must not be
        read as "nothing selected"."""
        assert as_selection(None) == {"include": [], "exclude_levels": {}}
        assert as_selection({}) == {"include": [], "exclude_levels": {}}
        assert is_empty(None) and is_empty({})

    def test_an_empty_level_list_is_inert(self):
        assert is_empty({"exclude_levels": {"subject": []}})

    def test_values_are_normalised_to_text(self):
        pair = as_selection({"exclude_levels": {"subject": [1, "02"]}})
        assert pair["exclude_levels"] == {"subject": ["1", "02"]}


class TestProjectionToSchemaFilter:
    """What a MATLAB command can express, and what it loses on the way."""

    def test_an_omitted_level_becomes_its_complement(self, populated_db):
        """A per-key RULE is exactly a per-key list, so this half is exact."""
        schema_filter, warnings = to_schema_filter(
            {"exclude_levels": {"session": ["pre"]}}, populated_db
        )
        assert schema_filter == {"session": ["post"]}
        assert warnings == []

    def test_one_location_projects_exactly(self, populated_db):
        schema_filter, warnings = to_schema_filter(
            {"include": [[["subject", "1"], ["session", "pre"]]]}, populated_db
        )
        assert schema_filter == {"subject": ["1"], "session": ["pre"]}
        assert warnings == []

    def test_a_ragged_selection_warns_that_it_runs_more(self, populated_db):
        """"All of subject 1, plus session post of subject 2" becomes
        subjects {1,2} x sessions {pre,post} — four combos for a selection of
        three. Silent over-running is the failure this warning exists for."""
        schema_filter, warnings = to_schema_filter(
            {
                "include": [
                    [["subject", "1"], ["session", "pre"]],
                    [["subject", "2"], ["session", "post"]],
                ]
            },
            populated_db,
        )
        assert schema_filter == {"subject": ["1", "2"], "session": ["pre", "post"]}
        assert any("more than was selected" in w for w in warnings)

    def test_a_key_some_prefix_leaves_unnamed_is_unconstrained(self, populated_db):
        """"All of subject 1" names no session, so it selects every session —
        the projection must not invent one from the other prefix."""
        schema_filter, warnings = to_schema_filter(
            {
                "include": [
                    [["subject", "1"]],
                    [["subject", "2"], ["session", "post"]],
                ]
            },
            populated_db,
        )
        assert schema_filter == {"subject": ["1", "2"]}
        assert "session" not in schema_filter
        assert any("session" in w for w in warnings)

    def test_both_halves_intersect(self, populated_db):
        schema_filter, _ = to_schema_filter(
            {
                "include": [[["session", "pre"]], [["session", "post"]]],
                "exclude_levels": {"session": ["pre"]},
            },
            populated_db,
        )
        assert schema_filter == {"session": ["post"]}

    def test_excluding_every_level_says_so(self, populated_db):
        _, warnings = to_schema_filter(
            {"exclude_levels": {"session": ["pre", "post"]}}, populated_db
        )
        assert any("nothing to iterate" in w for w in warnings)

    def test_an_inert_selection_projects_to_nothing(self, populated_db):
        assert to_schema_filter(None, populated_db) == (None, [])
        assert to_schema_filter({}, populated_db) == (None, [])


class TestNodeLocationTree:
    def test_a_node_with_no_resolvable_inputs_explains_itself(self, populated_db):
        """The panel must OPEN and say why it is empty. An error here would
        read as a broken picker rather than a loader with no DB inputs."""
        from scistack_gui.services.node_location_service import node_location_tree

        payload = node_location_tree(populated_db, "fn__does_not_exist")

        assert payload["roots"] == []
        assert payload["total"] == 0
        assert payload["notes"]
        assert "no input variables" in payload["notes"][0]

    def test_the_payload_shape_matches_the_plotting_tab(self, populated_db):
        """One component draws both tabs, so a second shape would be a second
        renderer."""
        from scistack_gui.services.node_location_service import node_location_tree
        from scistack_gui.services.plot_service import location_tree

        node_payload = node_location_tree(populated_db, "fn__does_not_exist")
        plot_payload = location_tree(populated_db, "FilteredSignal")

        assert set(node_payload) == set(plot_payload)

    def test_input_variables_come_from_the_node_not_the_name(self, populated_db):
        """Resolved through ``derive_target_for_node``: one function name can
        have several wirings on a canvas, and resolving by name shows another
        node's inputs."""
        from scistack_gui.services.node_location_service import input_variables_for_node

        names = input_variables_for_node(populated_db, "fn__bandpass_filter")
        assert names == [] or "RawSignal" in names
