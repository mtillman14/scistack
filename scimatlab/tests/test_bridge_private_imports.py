"""``scidb.run.external_loop`` exports what the MATLAB bridge calls (cleanup-audit F2)."""


def test_the_seam_exports_what_the_bridge_calls():
    from scidb.run import external_loop

    for name in (
        "prepare",
        "save_resolved",
        "build_skip_hook",
        "resolve_for_columns",
        "endpoint_policy",
        "apply_introspect",
    ):
        assert callable(getattr(external_loop, name)), name
