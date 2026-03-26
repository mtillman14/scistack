#!/bin/bash
# Install all packages in editable mode (dependency order)
set -e

echo "Installing all SciStack packages in editable mode..."

# Layer 0: no internal deps
pip install -e ./canonical-hash
pip install -e ./path-gen
pip install -e ./pipelinedb-lib
pip install -e ./scifor
pip install -e ./scirun-lib
pip install -e ./sciduck

# Layer 1: depends on canonicalhash
pip install -e ./thunk-lib

# Layer 2: depends on thunk, scipathgen, canonicalhash, sciduckdb, scirun
pip install -e ./scidb

# Layer 3: depends on scidb
pip install -e ./sci-matlab
pip install -e ./scidb-net

echo "All packages installed in editable mode."
