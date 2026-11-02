"""Pytest configuration and shared path setup for scistack tests."""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent  # /workspace
sys.path.insert(0, str(_root / "scistack" / "src"))
sys.path.insert(0, str(_root / "scidb" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scifor" / "src"))
