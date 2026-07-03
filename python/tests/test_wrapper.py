"""Wrapper tests against the real binary and the committed fixtures.

Run from the repo root with the binary built:

    cargo build --release
    DX_BIN=target/release/delta-explain python3 python/tests/test_wrapper.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delta_explain import DeltaExplainError, explain  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
BINARY = os.environ.get("DX_BIN", str(REPO / "target" / "release" / "delta-explain"))
TABLE = str(REPO / "fixtures" / "test-table")


def test_basic_report():
    r = explain(TABLE, where="country = 'DE' AND age > 40", binary=BINARY)
    assert r.schema_version.startswith("0.2.")
    assert r.total_files == 6
    assert r.final_files == 1
    assert r.passed and r.result is None
    assert r["analysis"]["confidence"] == "conservative"


def test_gate_failure_is_a_report_not_an_error():
    r = explain(TABLE, where="country = 'DE'", min_pruning=99, binary=BINARY)
    assert not r.passed
    assert r.result == "fail"
    assert r["assertions"][0]["name"] == "min_pruning"


def test_verbose_files_and_limit():
    r = explain(TABLE, where="country = 'DE'", verbose=True, limit=2, binary=BINARY)
    assert len(r.files) == 2
    assert r["files_truncated"] is True
    assert {"path", "kept", "pruned_by"} <= set(r.files[0])


def test_runtime_error_raises():
    try:
        explain(str(REPO / "no-such-table"), binary=BINARY)
    except DeltaExplainError as e:
        assert "Invalid path" in str(e)
    else:
        raise AssertionError("expected DeltaExplainError")


def test_options_mapping():
    # A bogus option key is ignored by the store layer; the call still works
    # and proves the KEY=VALUE plumbing.
    r = explain(TABLE, options={"region": "eu-central-1"}, binary=BINARY)
    assert r.total_files == 6


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {e}")
    sys.exit(1 if failures else 0)
