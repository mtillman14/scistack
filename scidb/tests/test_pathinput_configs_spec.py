"""pathinput_configs rebuilds a recorded PathInput with ALL its options.

2026-10-21: it copied name/template/root_folder out of the stored spec and
dropped regex/aliases/key_regex, so the should-run check of a loader whose
folders and files spell a session differently (``Baseline`` / ``_BL_``)
re-discovered 0 files. It now rebuilds through ``PathInput.from_spec``.
"""

from scidb.locations import pathinput_configs

from scidb import BaseVariable, configure_database, for_each, scistack
from scifor import PathInput

ALIASES = {"session": {"BL": ["Baseline"]}}
KEY_REGEX = {"subject": r"S\d{2}"}


class PiCfgLoaded(BaseVariable):
    schema_version = 1


@scistack
def import_pi_cfg(filepath):
    with open(filepath) as fh:
        return float(fh.read().strip())


def test_recorded_pathinput_keeps_aliases_and_key_regex(tmp_path):
    data = tmp_path / "data"
    d = data / "S01" / "Baseline"
    d.mkdir(parents=True)
    (d / "S01_BL.txt").write_text("1.0")
    pi = PathInput(
        "{subject}/{session}/{subject}_{session}.txt",
        root_folder=str(data),
        name="pi_cfg",
        aliases=ALIASES,
        key_regex=KEY_REGEX,
    )

    db = configure_database(tmp_path / "pi_cfg.duckdb", ["subject", "session"])
    try:
        for_each(import_pi_cfg, {"filepath": pi}, [PiCfgLoaded], subject=[], session=[])
        (inputs, _constants), = pathinput_configs(db._duck, "import_pi_cfg")
        rebuilt = inputs["filepath"]
        assert rebuilt.aliases == ALIASES
        assert rebuilt.key_regex == KEY_REGEX
        assert [(c["subject"], c["session"]) for c in rebuilt.discover()] == [("S01", "BL")]
    finally:
        db.close()
