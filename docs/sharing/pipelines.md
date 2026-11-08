# Share Part of a Project

<!-- Code variant of sharing/pipelines.ui.md (there: hypotheses and
     submodules; hypotheses are canvas tabs and have no code form, a
     submodule's code form is a Pipeline). Ground truth: scidb/run/pipeline.py
     (Pipeline(name) registers via pipe.for_each; use / uses=; bind(key_map=,
     params=, iterate=) -> PipelineBinding, never modifies its source, params
     "fn.param"; run_all = own steps + ancestors, so a pipeline that only uses
     another runs it with run_until(target)), scimatlab +scidb/Pipeline.m
     (constructor activates; bind('key_map', struct) ; key_map does not rewrite
     a MATLAB step's where=), scifor/project/names.py function_name (copied
     files keep bare names; libraries are lib.fn). -->

You don't have to share a whole project to share a piece of it. A **pipeline** (a group of `for_each` steps) and the functions and Variables it uses are ordinary code files. Send the files, and the other person runs them inside their own project.

Unlike the GUI's hypothesis and submodule files, which hold only the wiring, a code file holds the wiring **and** the code. What doesn't travel is everything that lives in your project rather than in the file: your Parameter and path input values, your settings, your run history and your results. To send those too, [export the whole project](export-import.md).

## Write the pipeline so it can travel

Put the pipeline, its functions and its Variables in one module, and give it a name with `scidb.Pipeline`. Register the steps with `pipe.for_each`, so that importing the module only **describes** the pipeline: nothing runs, and it doesn't need a database yet.

/// tab | Python

```python
# emg_preprocessing.py
from scidb import BaseVariable, Pipeline


class RawEMG(BaseVariable):
    """One trial's raw EMG signal."""

class FilteredEMG(BaseVariable):
    """The band-passed signal."""

class EMGEnvelope(BaseVariable):
    """The RMS envelope."""


def bandpass(signal, low_hz=20, high_hz=450):
    ...

def rms_envelope(signal, window_ms=50):
    ...


preprocessing = Pipeline("emg_preprocessing")
preprocessing.for_each(bandpass, inputs={"signal": RawEMG}, outputs=[FilteredEMG],
                       subject=[], session=[])
preprocessing.for_each(rms_envelope, inputs={"signal": FilteredEMG}, outputs=[EMGEnvelope],
                       subject=[], session=[])
```

///

/// tab | MATLAB

A MATLAB pipeline starts collecting steps as soon as it's created, so wrap it in a function that returns it:

```matlab
% emg_preprocessing.m
function pipe = emg_preprocessing()
    pipe = scidb.Pipeline("emg_preprocessing");
    pipe.for_each(@bandpass, struct('signal', RawEMG()), {FilteredEMG()}, ...
        subject=[], session=[]);
    pipe.for_each(@rms_envelope, struct('signal', FilteredEMG()), {EMGEnvelope()}, ...
        subject=[], session=[]);
    pipe.deactivate();
end
```

Send `bandpass.m`, `rms_envelope.m` and the Variable class files (`RawEMG.m`, `FilteredEMG.m`, `EMGEnvelope.m`) with it.

///

The schema keys named in the steps (`subject`, `session`) are **yours**. The recipient adapts them below. Your functions never see the schema, so they need no changes.

Tell the recipient which packages the functions import, so they can add them to their `pyproject.toml` (a copied file brings no dependency list).

## Use it in another project

The recipient copies the file into their project, next to their own code, and composes it into a pipeline of theirs with `use`:

/// tab | Python

```python
import scidb
from emg_preprocessing import EMGEnvelope, preprocessing

db = scidb.configure_database("study.duckdb", ["participant", "trial"])

study = db.pipeline("study", uses=[
    preprocessing.bind(
        key_map={"subject": "participant", "session": "trial"},
        params={"low_hz": 30},
    ),
])
study.run_until(EMGEnvelope)
```

///

/// tab | MATLAB

```matlab
db = scidb.configure_database("study.duckdb", ["participant", "trial"]);
preprocessing = emg_preprocessing();

study = scidb.Pipeline("study", 'uses', {preprocessing.bind( ...
    'key_map', struct('subject', 'participant', 'session', 'trial'), ...
    'params',  struct('low_hz', 30))});
study.run_until(EMGEnvelope());
```

///

- **`key_map`** renames your schema keys to theirs: every step's iteration keys, path templates and structured `where=` filters are rewritten, and the results are saved under their keys. Keys with the same name in both schemas need no entry. In MATLAB, a step's `where=` filter is not rewritten yet: adjust it by hand.
- **`params`** changes a setting for this use only. A bare name (`low_hz`) must match exactly one function's input; `"bandpass.low_hz"` names it precisely. A different setting is a different computation, so its results are kept separately from any made with the original value.
- **`iterate`** overrides which values a step iterates over, using **their** key names.

`bind` never changes `preprocessing` itself, so the same pipeline can be bound one way in one place and another way in another.

**Run it with `run_until`.** `study.run_all()` runs only the steps `study` registers itself, plus whatever they need. A pipeline that only `use`s another registers none, so name the result you want: `run_until(EMGEnvelope)` runs every step that leads to it. Once `study` has steps of its own that read `EMGEnvelope`, `run_all()` runs the preprocessing too.

## What to watch for

- **Names.** The copied functions become the recipient's own and are recorded under their bare names (`bandpass`). If their project already has a different `bandpass`, rename one of them. A [library](libraries.md) avoids this, because its functions are recorded as `emg_tools.bandpass`.
- **Variables.** A Variable in the file and one in their project with the same name and the same declaration are the same Variable. If they're declared differently, SciStack says so and names both.
- **Parameter and path input values** come from the recipient's own entities file, never from yours. Tell them which ones the pipeline needs.
- **It's a copy.** Your later changes don't reach their file. To keep improving the code for several projects, make it a [library](libraries.md) instead, and they get each new version by installing it.

## For someone who doesn't use SciStack

Your functions are ordinary functions: they take values and return values, and never see the schema or the database. Send the file with the functions alone (without the `Pipeline` and `for_each` lines) and they work in any Python or MATLAB code.

To give them a whole runnable analysis without a database, [scifor](../examples/best-popcorn-scifor.md) runs the same functions over tables in memory, with no database or project to set up.

## Next steps

- [What Can I Share?](overview.md): which way to share, and what travels with each.
- [Libraries](libraries.md): the same pipeline as an installable package that several projects use and keep up to date.
- [Pipelines](../api/scidb.md#pipelines): every method of `Pipeline`, including `plan` to see what would run.
