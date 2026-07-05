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
EXE = ".exe" if sys.platform == "win32" else ""
BINARY = os.environ.get(
    "DX_BIN", str(REPO / "target" / "release" / f"delta-explain{EXE}")
)
TABLE = str(REPO / "fixtures" / "test-table")

# A stub binary that echoes its argv inside a minimal valid report, so the
# flag plumbing of every kwarg is assertable without cloud credentials.
STUB = Path(__file__).parent / "_argv_stub.py"


def test_basic_report():
    r = explain(TABLE, where="country = 'DE' AND age > 40", binary=BINARY)
    assert r.schema_version.startswith("0.3.")
    assert r.total_files == 6
    assert r.final_files == 1
    assert r.passed and r.result is None
    assert r["analysis"]["confidence"] == "conservative"


def test_prefix_like_rewrites_to_a_range():
    r = explain(TABLE, where="country LIKE 'D%'", binary=BINARY)
    assert r["analysis"]["partition_safe"] == "country >= 'D' AND country < 'E'"
    assert r["analysis"]["confidence"] == "exact"
    assert r.final_files == 2


def test_partition_only_like_is_evaluated_exactly():
    r = explain(TABLE, where="country LIKE '%E'", binary=BINARY)
    assert r["analysis"]["partition_exact"] == "country LIKE '%E'"
    assert r["analysis"]["partition_safe"] is None
    assert r["analysis"]["confidence"] == "exact"
    assert r["analysis"]["notes"] == []
    assert r.final_files == 2


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


def test_every_kwarg_reaches_the_cli():
    from delta_explain import explain

    r = explain(
        "s3://bucket/table",
        where="a = 1",
        min_pruning=80,
        assert_stats=True,
        at_version=3,
        verbose=True,
        limit=5,
        env_creds=True,
        profile="p1",
        region="eu-central-1",
        public=True,
        options={"endpoint": "http://x", "allow_http": "true"},
        binary=[sys.executable, str(STUB)],
    )
    argv = r["argv"]
    assert argv[0] == "s3://bucket/table"
    for expected in (
        ["--format", "json"],
        ["--where", "a = 1"],
        ["--min-pruning", "80"],
        ["--assert-stats"],
        ["--at-version", "3"],
        ["--verbose"],
        ["--limit", "5"],
        ["--env-creds"],
        ["--profile", "p1"],
        ["--region", "eu-central-1"],
        ["--public"],
        ["--option", "endpoint=http://x"],
        ["--option", "allow_http=true"],
    ):
        joined = " ".join(argv)
        assert " ".join(expected) in joined, f"missing {expected} in {argv}"


def test_report_is_a_mapping_with_typed_accessors():
    from delta_explain import Report

    r = Report(
        {
            "schema_version": "0.2.0",
            "total_files": 6,
            "final_files": 2,
            "total_pruning_pct": 66.6,
            "result": "fail",
        }
    )
    assert r["total_files"] == 6
    assert len(r) == 5 and "result" in set(iter(r))
    assert dict(r)["final_files"] == 2
    assert r.schema_version == "0.2.0"
    assert not r.passed and r.result == "fail"
    assert r.files is None  # verbose-only field absent


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
