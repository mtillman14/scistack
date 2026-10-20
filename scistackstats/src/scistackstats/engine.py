"""The ONLY module in scistackstats that imports ``csvstats`` (plan P9).

csv-stats computes every number: test statistics, p-values, effect sizes,
corrections and descriptive statistics (S6, S18). scistackstats builds the
design and calls this seam. Unit tests swap in :class:`FakeEngine` with
:func:`use_engine`.

A test csv-stats cannot run comes back as :class:`EngineRefusal`, never as a
fallback to scipy or pingouin:

- ``kind="gap"``: the design is valid, but csv-stats does not support it yet
  (``csvstats.core.NotSupported``). csv-stats is grown alongside StatStudio,
  so a gap is a to-do there, logged as ``csv-stats gap: …``.
- ``kind="invalid"``: the data cannot support the test (fewer than two
  values, a duplicated subject row, …). csv-stats' message is kept.

The runner records either as a refusal on that stratum and never raises.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator, Protocol, Sequence

import pandas as pd
from scistacklog import Log

LAYER = "scistackstats"

#: Tests that compare the two levels of one factor.
PAIRWISE_TESTS = (
    "ttest_ind_welch", "ttest_ind_student", "ttest_paired", "mann_whitney", "wilcoxon",
    "spm_ttest2", "spm_ttest_paired", "lmm_pairwise",
)
#: Omnibus tests over every factor of a design.
OMNIBUS_TESTS = ("anova", "kruskal", "friedman")


def _version_tuple(version: str) -> tuple[int, ...]:
    """``"0.4.0"`` -> ``(0, 4, 0)``; ``"0.3.0rc1"`` -> ``(0, 3, 0)``: each part's
    LEADING digits only (joining every digit read "0rc1" as 1), and a part
    with none stops the tuple."""
    import re

    out = []
    for part in str(version).split("."):
        match = re.match(r"\d+", part)
        if match is None:
            break
        out.append(int(match.group()))
    return tuple(out)


class EngineRefusal(Exception):
    """csv-stats could not run this test here; ``reason`` says why."""

    def __init__(self, reason: str, *, kind: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.kind = kind


class FormulaSyntaxError(ValueError):
    """csv-stats' grammar refused a model formula. ``position``/``length``:
    the offending span of the text, for the GUI to underline."""

    def __init__(self, reason: str, position: int, length: int = 1) -> None:
        super().__init__(reason)
        self.reason = reason
        self.position = position
        self.length = length


def _formula_grammar():
    """csv-stats' formula grammar (``csvstats.lmm_formula``, the one owner),
    or a "gap" refusal on a csv-stats without it."""
    try:
        from csvstats import lmm_formula
    except ImportError as exc:
        raise EngineRefusal("not yet in csv-stats: model formulas need csvstats.lmm_formula", kind="gap") from exc
    return lmm_formula


def parse_formula(text: str) -> dict[str, Any]:
    """``{"response", "fixed": [...], "random": [{"group", "terms",
    "correlated"}], "names": [...]}``, or :class:`FormulaSyntaxError`.
    Shared by every engine: the grammar is csv-stats', never restated."""
    grammar = _formula_grammar()
    try:
        m = grammar.parse(text)
    except grammar.FormulaError as exc:
        raise FormulaSyntaxError(exc.reason, exc.position, exc.length) from exc
    return {
        "response": m.response, "fixed": list(m.fixed), "names": list(m.names),
        "random": [{"group": list(r.group), "terms": list(r.terms), "correlated": r.correlated} for r in m.random],
    }


def render_formula(fixed: Sequence[str], random: Sequence[dict[str, Any]], response: str | None = None) -> str:
    """The canonical text of a parsed formula (``parse_formula``'s shape)."""
    grammar = _formula_grammar()
    from csvstats.lmm_fit import RandomTerm

    terms = tuple(RandomTerm(tuple(r["group"]), tuple(r["terms"]), bool(r.get("correlated", True))) for r in random)
    return grammar.render(grammar.ModelFormula(fixed=tuple(fixed), random=terms), response=response)


class Engine(Protocol):
    name: str

    def run(
        self,
        test: str,
        data: pd.DataFrame,
        dv: str,
        *,
        between: Sequence[str] = (),
        within: Sequence[str] = (),
        subject: str | None = None,
        levels: dict[str, list[Any]] | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    def assumptions(
        self,
        data: pd.DataFrame,
        dv: str,
        *,
        between: Sequence[str] = (),
        within: Sequence[str] = (),
        subject: str | None = None,
        levels: dict[str, list[Any]] | None = None,
        alpha: float = 0.05,
    ) -> list[dict[str, Any]]: ...

    def correct(self, pvalues: Sequence[float | None], method: str) -> list[float | None]: ...

    def summarize(self, values: Any) -> dict[str, Any]: ...

    def corrections(self) -> tuple[str, ...]: ...

    def versions(self) -> dict[str, str]: ...


class CsvStatsEngine:
    """The real engine: :mod:`csvstats.core` and :mod:`csvstats.descriptive`."""

    name = "csvstats"

    def __init__(self) -> None:
        import csvstats
        from csvstats import core, descriptive

        self._core = core
        self._descriptive = descriptive
        self._version = csvstats.__version__
        Log.info("engine csvstats %s", self._version, layer=LAYER)

    def run(self, test, data, dv, *, between=(), within=(), subject=None, levels=None, options=None):
        core = self._core
        opts = dict(options or {})
        alpha = opts.get("alpha", 0.05)
        ci_level = opts.get("ci_level", 0.95)
        tails = opts.get("tails", "two-sided")
        design = dict(between=list(between), within=list(within), subject=subject, levels=levels)
        try:
            if test in ("ttest_ind_welch", "ttest_ind_student", "ttest_paired"):
                return core.ttest(
                    data, dv, **design, equal_var=(test == "ttest_ind_student"),
                    alpha=alpha, ci_level=ci_level, tails=tails,
                )
            if test in ("mann_whitney", "wilcoxon"):
                return core.rank_test(data, dv, **design, alpha=alpha, ci_level=ci_level, tails=tails)
            if test in ("spm_ttest2", "spm_ttest_paired"):
                return core.spm_ttest(data, dv, **design, alpha=alpha, ci_level=ci_level, tails=tails)
            if test == "anova":
                return core.anova(data, dv, **design, alpha=alpha, ci_level=ci_level)
            if test in ("lmm", "lmm_pairwise"):
                # Every observation used; Satterthwaite df and full-model
                # contrasts need csv-stats 0.4 (0.3 had Wald tests and a
                # per-pair lmm_pairwise without factor=).
                needed = "mixed models need csv-stats>=0.4.0 (Satterthwaite df, full-model contrasts)"
                if _version_tuple(self._version) < (0, 4):
                    raise EngineRefusal(f"not yet in csv-stats {self._version}: {needed}", kind="gap")
                try:
                    from csvstats.lmm import lmm, lmm_pairwise
                except ImportError as exc:
                    raise EngineRefusal(f"not yet in csv-stats {self._version}: {needed}", kind="gap") from exc

                # A model formula's covariates and random terms
                # (scistackstats.model); absent = csv-stats' (1 | subject).
                model = {k: opts[k] for k in ("covariates", "random") if opts.get(k)}
                if test == "lmm":
                    return lmm(data, dv, **design, **model, alpha=alpha, ci_level=ci_level)
                # Full-model contrasts (csv-stats >= 0.4): ONE call returns every pair of
                # `factor` within each combination of the other factors.
                return lmm_pairwise(data, dv, **design, **model, factor=opts.get("factor"), alpha=alpha,
                                    ci_level=ci_level, tails=tails)
            if test == "kruskal":
                return core.kruskal(data, dv, between=list(between), levels=levels, alpha=alpha, ci_level=ci_level)
            if test == "friedman":
                return core.friedman(
                    data, dv, within=list(within), subject=subject, levels=levels,
                    alpha=alpha, ci_level=ci_level,
                )
        except core.NotSupported as exc:
            Log.info("csv-stats gap: %s — %s", test, exc, layer=LAYER)
            raise EngineRefusal(f"not yet in csv-stats: {exc}", kind="gap") from exc
        except ValueError as exc:
            Log.info("csv-stats refused %s: %s", test, exc, layer=LAYER)
            raise EngineRefusal(str(exc), kind="invalid") from exc
        raise EngineRefusal(f"not yet in csv-stats: no test {test!r}", kind="gap")

    def assumptions(self, data, dv, *, between=(), within=(), subject=None, levels=None, alpha=0.05):
        """csv-stats' assumption checks (``core.assumption_checks``), for the
        recommendation. A check that cannot run is an empty list, not an error."""
        check = getattr(self._core, "assumption_checks", None)
        if check is None:
            Log.warn("csv-stats %s has no assumption_checks (needs >=0.3.0): recommendations "
                     "fall back to parametric", self._version, layer=LAYER)
            return []
        try:
            out = check(
                data, dv, between=list(between), within=list(within), subject=subject,
                levels=levels, alpha=alpha,
            )
        except (ValueError, self._core.NotSupported) as exc:
            Log.info("csv-stats assumption checks unavailable: %s", exc, layer=LAYER)
            return []
        return list(out.get("assumptions", []))

    def correct(self, pvalues, method):
        return self._core.correct_pvalues(list(pvalues), method)

    def summarize(self, values):
        return self._descriptive.summarize(values)

    def corrections(self):
        return tuple(self._core.CORRECTIONS)

    def versions(self):
        import scipy

        out = {"csvstats": self._version, "scipy": scipy.__version__}
        try:
            import pingouin

            out["pingouin"] = pingouin.__version__
        except ImportError:  # pragma: no cover - csv-stats[full] brings it
            pass
        try:
            import statsmodels

            out["statsmodels"] = statsmodels.__version__
        except ImportError:  # pragma: no cover - csv-stats[full] brings it
            pass
        return out


class FakeEngine:
    """A deterministic stand-in for unit tests (P9).

    Test statistics are fake: the estimate is the real ``mean(a) - mean(b)``,
    but ``p`` comes from ``p_values`` (keyed by test id, or by
    ``(test, a, b)`` for a pairwise test, or ``(test, term)`` for an omnibus
    term), defaulting to ``default_p``. Descriptives and corrections are the
    real csv-stats ones, so a fake engine never invents a summary number.
    """

    name = "fake"

    def __init__(self, p_values: dict | None = None, default_p: float = 0.04, refuse: dict | None = None,
                 exclude: dict | None = None, violated: tuple = ()) -> None:
        from csvstats import core, descriptive

        self._core = core
        self._descriptive = descriptive
        self.p_values = dict(p_values or {})
        self.default_p = default_p
        #: ``{test id: (reason, kind)}`` to simulate a refusal.
        self.refuse = dict(refuse or {})
        #: ``{test id: [(unit, reason), ...]}`` to simulate csv-stats leaving
        #: units out (``n.excluded_detail``), for the never-silent tests.
        self.exclude = dict(exclude or {})
        #: Assumption names the fake checks report as violated.
        self.violated = set(violated)
        self.calls: list[dict[str, Any]] = []

    def _lmm_pairwise(self, test, data, dv, factors, between, within, subject, levels, options):
        """csv-stats' shape (>= 0.4): every pair of ``options["factor"]``
        within each combination of the other factors, from one call. The
        estimate is the real difference of cell means; p is fake as usual."""
        factor = options.get("factor") or factors[0]
        levels = levels or {}
        lv = levels.get(factor) or sorted(data[factor].unique())
        others = [f for f in factors if f != factor]
        contexts = [{}]
        for o in others:
            contexts = [{**c, o: v} for c in contexts for v in (levels.get(o) or sorted(data[o].unique()))]
        effects = []
        for context in contexts:
            part = data
            for k, v in context.items():
                part = part[part[k] == v]
            for i in range(len(lv)):
                for j in range(i + 1, len(lv)):
                    a, b = lv[i], lv[j]
                    diff = float(part.loc[part[factor] == a, dv].mean() - part.loc[part[factor] == b, dv].mean())
                    effects.append({
                        "term": factor, "a": str(a), "b": str(b), "context": {k: str(v) for k, v in context.items()},
                        "statistic_name": "t", "statistic": diff, "df": [float(len(data) - 2)],
                        "df_method": "fake", "p": self._p((test, str(a), str(b)), test),
                        "estimate": {"name": "mean_difference", "value": diff, "ci": [diff - 1, diff + 1],
                                     "se": 0.5, "sd_of_differences": None, "method": "fake"},
                        "effect_size": None, "effect_sizes": {},
                    })
        return {
            "schema_version": 1, "test": test, "test_label": f"fake {test}",
            "design": {"dv": dv, "between": list(between), "within": list(within), "subject": subject},
            "n": self._n(test, data, subject), "effects": effects, "assumptions": [], "warnings": [],
            "engine": self.versions(), "details": {},
        }

    def _n(self, test, data, subject) -> dict[str, Any]:
        """csv-stats' ``n`` shape; ``exclude`` drops the named units."""
        dropped = [(str(u), r) for u, r in self.exclude.get(test, [])]
        units = None  # unset unless simulating, so existing fake n stays as it was
        if dropped and subject and subject in data.columns:
            units = int(data[subject].nunique()) - len(dropped)
        return {
            "total": int(len(data)), "subjects": units, "per_cell": [], "excluded": len(dropped),
            "excluded_detail": [{"subject": u, "reason": r} for u, r in dropped],
        }

    def _p(self, *keys) -> float:
        for key in keys:
            if key in self.p_values:
                return self.p_values[key]
        return self.default_p

    def run(self, test, data, dv, *, between=(), within=(), subject=None, levels=None, options=None):
        self.calls.append({"test": test, "between": list(between), "within": list(within),
                           "subject": subject, "levels": levels, "rows": len(data),
                           "columns": list(data.columns),
                           "covariates": list((options or {}).get("covariates") or []),
                           "random": list((options or {}).get("random") or [])})
        if test in self.refuse:
            reason, kind = self.refuse[test]
            raise EngineRefusal(reason, kind=kind)
        factors = [*between, *within]
        if test == "lmm_pairwise":
            return self._lmm_pairwise(test, data, dv, factors, between, within, subject, levels, options or {})
        if test in PAIRWISE_TESTS:
            (factor,) = factors
            a, b = (levels or {})[factor]
            if test.startswith("spm_"):
                # Curves: no scalar mean; a fixed fake cluster so the report has one.
                ma = mb = 0.0
            else:
                ma = float(data.loc[data[factor] == a, dv].mean())
                mb = float(data.loc[data[factor] == b, dv].mean())
            effect = {
                "term": factor, "a": str(a), "b": str(b), "statistic_name": "t",
                "statistic": ma - mb, "df": [float(len(data) - 2)], "df_method": "fake",
                "p": self._p((test, str(a), str(b)), test),
                "estimate": {"name": "mean_difference", "value": ma - mb, "ci": [ma - mb - 1, ma - mb + 1],
                             "se": 0.5, "sd_of_differences": 1.0, "method": "fake"},
                "effect_size": {"name": "g_s", "value": 0.5, "ci": [0.1, 0.9]},
                "effect_sizes": {name: {"name": name, "value": 0.5, "ci": None}
                                 for name in ("d_s", "g_s", "d_z", "d_av", "d_rm", "rank_biserial")},
            }
            if test.startswith("spm_"):
                effect.update(clusters=[{"start": 40.0, "end": 60.0, "p": effect["p"], "extent": 20.0}],
                              threshold=3.0, effect_size=None, effect_sizes={})
            effects = [effect]
        else:
            terms = list(factors)
            if len(factors) >= 2:
                terms.append(":".join(factors))
            effects = [{
                "term": term, "statistic_name": "F", "statistic": 3.0, "df": [1.0, 10.0],
                "df_method": "fake", "p": self._p((test, term), test), "estimate": None,
                "effect_size": {"name": "eta2_p", "value": 0.2, "ci": None},
                "effect_sizes": {name: {"name": name, "value": 0.2, "ci": None}
                                 for name in ("eta2_p", "eta2_g", "omega2_p", "epsilon2", "kendall_w")},
            } for term in terms]
        return {
            "schema_version": 1, "test": test, "test_label": f"fake {test}",
            "design": {"dv": dv, "between": list(between), "within": list(within), "subject": subject},
            "n": self._n(test, data, subject),
            "effects": effects, "assumptions": [], "warnings": [], "engine": self.versions(), "details": {},
        }

    def assumptions(self, data, dv, *, between=(), within=(), subject=None, levels=None, alpha=0.05):
        """``violated`` from the ``violated`` constructor set (names
        "normality" / "homogeneity_of_variance"); every check passes by default."""
        checks = [{"name": "normality", "test": "shapiro_wilk", "scope": "residuals",
                   "statistic": 0.95, "p": 0.001 if "normality" in self.violated else 0.5,
                   "violated": "normality" in self.violated}]
        if between:
            checks.append({"name": "homogeneity_of_variance", "test": "levene", "scope": "groups",
                           "statistic": 1.0, "p": 0.001 if "homogeneity_of_variance" in self.violated else 0.5,
                           "violated": "homogeneity_of_variance" in self.violated})
        return checks

    def correct(self, pvalues, method):
        return self._core.correct_pvalues(list(pvalues), method)

    def summarize(self, values):
        return self._descriptive.summarize(values)

    def corrections(self):
        return tuple(self._core.CORRECTIONS)

    def versions(self):
        return {"fake": "1"}


_ENGINE: ContextVar[Engine | None] = ContextVar("scistackstats_engine", default=None)
_DEFAULT: list[Engine] = []


def get_engine() -> Engine:
    """The engine in use: a :func:`use_engine` override, else csv-stats."""
    engine = _ENGINE.get()
    if engine is not None:
        return engine
    if not _DEFAULT:
        _DEFAULT.append(CsvStatsEngine())
    return _DEFAULT[0]


@contextmanager
def use_engine(engine: Engine) -> Iterator[Engine]:
    """Run the block with ``engine`` (tests use :class:`FakeEngine`)."""
    token = _ENGINE.set(engine)
    try:
        yield engine
    finally:
        _ENGINE.reset(token)
