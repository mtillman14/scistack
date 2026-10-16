# Create New Project

This page assumes that you are working VS Code and that you have already installed SciStack using MATLAB or Python.

1. Run `scistack init` in the Command Line from the project root to populate the standard set of folders.

2. Create a `main` code file in the `src` folder, and add the following required boilerplate:

/// tab | Python

```python
# main.py
import scidb

schema = ["subject", "session"] # Arbitrary schema keys
scidb.set_schema(schema)
```
///

/// tab | MATLAB

```matlab
% main.m

schema = ["subject", "session"] % Arbitrary schema keys
scidb.set_schema(schema)
```
///

That's it. The rest of the code will differ between projects. Refer to the {API} documentation for information on how SciStack helps to load, transform, visualize, summarize, and save any kind of data in any experiment format.
