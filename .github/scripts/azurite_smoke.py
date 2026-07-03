#!/usr/bin/env python3
"""Azurite smoke: write a partitioned Delta table to the Azure emulator via
delta-rs, then assert delta-explain's az:// path end to end (baseline count,
gate exit codes, degradation warning). This is the automated version of the
manual check that caught the az:// log-reader regression.

Expects: azurite reachable on 127.0.0.1:10000, DX_BIN pointing at the
delta-explain binary. Exits nonzero on any mismatch.
"""
import json
import os
import random
import subprocess
import sys

import pyarrow as pa
from azure.storage.blob import BlobServiceClient
from deltalake import write_deltalake

ACCOUNT = "devstoreaccount1"
KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/"
    "KBHBeksoGMGw=="
)
CONN = (
    f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};"
    f"BlobEndpoint=http://127.0.0.1:10000/{ACCOUNT};"
)
DX = os.environ.get("DX_BIN", "delta-explain")
DX_OPTS = [
    "--option", f"account_name={ACCOUNT}",
    "--option", f"account_key={KEY}",
    "--option", "use_emulator=true",
    "--option", "allow_http=true",
]


def dx(args, expect_exit):
    proc = subprocess.run([DX, *args], capture_output=True, text=True)
    if proc.returncode != expect_exit:
        print(f"FAIL: {args} exited {proc.returncode}, expected {expect_exit}")
        print(proc.stdout[-2000:], proc.stderr[-2000:], sep="\n---\n")
        sys.exit(1)
    return proc.stdout


def main():
    BlobServiceClient.from_connection_string(CONN).create_container("lake")

    random.seed(7)
    countries = ["DE", "US", "IT"]
    rows = [
        {"name": f"u{i}", "age": random.randint(18, 80), "country": random.choice(countries)}
        for i in range(600)
    ]
    tbl = lambda rs: pa.table({k: [r[k] for r in rs] for k in ("name", "age", "country")})
    write_opts = {
        "azure_storage_account_name": ACCOUNT,
        "azure_storage_account_key": KEY,
        "azure_storage_use_emulator": "true",
        "azure_allow_http": "true",
    }
    first = True
    for c in countries:
        crows = sorted([r for r in rows if r["country"] == c], key=lambda r: r["age"])
        chunk = max(1, len(crows) // 4)
        for k in range(0, len(crows), chunk):
            write_deltalake(
                "az://lake/users", tbl(crows[k : k + chunk]), partition_by=["country"],
                mode="overwrite" if first else "append", storage_options=write_opts,
            )
            first = False
    print("table written to azurite")

    out = dx(["az://lake/users", *DX_OPTS, "--format", "json"], 0)
    report = json.loads(out)
    assert report["total_files"] > 0, "empty snapshot"

    dx(["az://lake/users", *DX_OPTS, "-w", "country = 'DE' AND age > 55", "--min-pruning", "50"], 0)
    dx(["az://lake/users", *DX_OPTS, "-w", "country = 'DE' AND age > 55", "--min-pruning", "99"], 1)

    out = dx(["az://lake/users", *DX_OPTS, "-w", "country = 'DE' AND UPPER(name) = 'X'"], 0)
    assert "UNSUPPORTED_EXPRESSION" in out, "degradation warning missing"

    print("azurite smoke: OK (baseline, gates, degradation)")


if __name__ == "__main__":
    main()
