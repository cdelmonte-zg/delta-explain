"""Thin Python wrapper around the delta-explain CLI.

The wheel ships the compiled binary; this module invokes it and returns the
schema-versioned JSON report as a `Report`. One contract: everything the
module exposes is documented in docs/json-schema.md and guaranteed by
docs/semantics.md in the repository.

    from delta_explain import explain

    report = explain("s3://warehouse/events",
                     where="country = 'DE' AND age > 40",
                     min_pruning=80, env_creds=True)
    report.passed             # False also makes the CLI exit 1 in CI
    report.total_pruning_pct
    report["analysis"]["confidence"]
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sysconfig
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

__all__ = ["explain", "Report", "DeltaExplainError", "binary_path"]


class DeltaExplainError(RuntimeError):
    """A runtime failure: unreadable table, bad predicate, storage error.

    Gate failures (--min-pruning / --assert-stats) are NOT errors: they
    come back as a Report with `passed == False`, mirroring the CLI's
    exit-code contract (report on stdout, exit 1).
    """


class Report(Mapping[str, Any]):
    """The JSON report; a read-only mapping plus convenience accessors."""

    def __init__(self, raw: dict):
        self._raw = raw

    # Mapping protocol: report["analysis"]["confidence"] etc.
    def __getitem__(self, key: str) -> Any:
        return self._raw[key]

    def __iter__(self):
        return iter(self._raw)

    def __len__(self) -> int:
        return len(self._raw)

    @property
    def raw(self) -> dict:
        return self._raw

    @property
    def schema_version(self) -> str:
        return self._raw["schema_version"]

    @property
    def total_files(self) -> int:
        return self._raw["total_files"]

    @property
    def final_files(self) -> int:
        return self._raw["final_files"]

    @property
    def total_pruning_pct(self) -> float:
        return self._raw["total_pruning_pct"]

    @property
    def result(self) -> Optional[str]:
        """"pass" / "fail" from the gates, or None when no gate ran."""
        return self._raw["result"]

    @property
    def passed(self) -> bool:
        """True unless a gate failed. No gates counts as passed."""
        return self._raw["result"] != "fail"

    @property
    def files(self) -> Optional[list]:
        """Per-file outcomes; present only when verbose=True was requested."""
        return self._raw.get("files")

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Report(files={self.total_files}->{self.final_files}, "
            f"pruned={self.total_pruning_pct:.0f}%, result={self.result!r})"
        )


def binary_path() -> str:
    """The delta-explain binary this module will invoke.

    The one shipped inside this wheel (the scripts directory) wins; PATH is
    the fallback for source checkouts and custom setups.
    """
    bundled = Path(sysconfig.get_path("scripts")) / "delta-explain"
    for candidate in (bundled, bundled.with_suffix(".exe")):
        if candidate.is_file():
            return str(candidate)
    on_path = shutil.which("delta-explain")
    if on_path:
        return on_path
    raise DeltaExplainError(
        "delta-explain binary not found (neither bundled in this "
        "environment's scripts directory nor on PATH)"
    )


def explain(
    table: str,
    *,
    where: Optional[str] = None,
    min_pruning: Optional[float] = None,
    assert_stats: bool = False,
    at_version: Optional[int] = None,
    verbose: bool = False,
    limit: Optional[int] = None,
    env_creds: bool = False,
    profile: Optional[str] = None,
    region: Optional[str] = None,
    public: bool = False,
    options: Optional[Mapping[str, str]] = None,
    binary: Optional[str] = None,
) -> Report:
    """Run delta-explain against `table` and return the JSON report.

    Keyword arguments mirror the CLI flags one to one; `options` becomes
    repeated `--option KEY=VALUE` pairs. Gate failures return a Report with
    `passed == False`; runtime errors raise DeltaExplainError with the
    CLI's stderr message.
    """
    argv: list[str] = [binary or binary_path(), table, "--format", "json"]
    if where is not None:
        argv += ["--where", where]
    if min_pruning is not None:
        argv += ["--min-pruning", str(min_pruning)]
    if assert_stats:
        argv += ["--assert-stats"]
    if at_version is not None:
        argv += ["--at-version", str(at_version)]
    if verbose:
        argv += ["--verbose"]
    if limit is not None:
        argv += ["--limit", str(limit)]
    if env_creds:
        argv += ["--env-creds"]
    if profile is not None:
        argv += ["--profile", profile]
    if region is not None:
        argv += ["--region", region]
    if public:
        argv += ["--public"]
    for key, value in (options or {}).items():
        argv += ["--option", f"{key}={value}"]

    proc = subprocess.run(argv, capture_output=True, text=True)

    # The CLI contract (docs/semantics.md): stdout is a complete report or
    # empty. Exit 1 with a report is a gate failure; exit 1 with empty
    # stdout is a runtime error; exit 2 is a usage error.
    if proc.stdout.strip():
        return Report(json.loads(proc.stdout))
    raise DeltaExplainError(proc.stderr.strip() or f"exit code {proc.returncode}")
