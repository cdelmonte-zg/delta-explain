#!/usr/bin/env python3
"""Fake delta-explain: echoes argv inside a minimal valid JSON report."""
import json
import sys

print(json.dumps({
    "schema_version": "0.2.0",
    "total_files": 0,
    "final_files": 0,
    "total_pruning_pct": 0.0,
    "result": None,
    "argv": sys.argv[1:],
}))
