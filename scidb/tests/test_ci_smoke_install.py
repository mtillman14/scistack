"""The release smoke check (.github/scripts/smoke_install.py) must FAIL on a
missing module unless a package declares it as an optional extra.

Regression 2026-10-18: csv-stats 0.2.0 shipped an empty wheel; every
`import csvstats` raised ModuleNotFoundError and the check printed warnings
and passed, because only missing SciStack siblings counted as errors.
"""

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / ".github" / "scripts" / "smoke_install.py"


def _load():
    spec = importlib.util.spec_from_file_location("smoke_install", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeDist:
    def __init__(self, name, requires):
        self.metadata = {"Name": name}
        self.requires = requires


def test_extra_only_requirements_are_optional():
    smoke = _load()
    dists = [
        _FakeDist("scistackplot", [
            "numpy>=1.20",
            "csv-stats>=0.2.1",
            'seaborn>=0.13; extra == "mpl"',
            'plotly>=5.0; extra == "interactive"',
        ]),
    ]
    optional = smoke.optional_imports(dists)
    assert optional == {"seaborn": "scistackplot[mpl]", "plotly": "scistackplot[interactive]"}
    assert "csv_stats" not in optional  # a required dependency is never optional


def test_required_by_one_package_is_not_optional():
    smoke = _load()
    dists = [
        _FakeDist("a", ['seaborn; extra == "mpl"']),
        _FakeDist("b", ["seaborn>=0.13"]),
    ]
    assert "seaborn" not in smoke.optional_imports(dists)


def test_import_name_spelling():
    smoke = _load()
    assert smoke.import_name_of("tomli-w") == "tomli_w"
    assert smoke.import_name_of("PyYAML") == "yaml"


def test_provider_hint_names_an_installed_distribution():
    """pytest is installed here; asking for a module spelled like it ('py-test'
    flattens to 'pytest') must name the installed distribution."""
    smoke = _load()
    assert "IS installed but provides no" in smoke.provider_hint("py_test")
    assert "declares it as an optional extra" in smoke.provider_hint("no_such_module_x")
