#!/usr/bin/env python3
"""Azurite smoke: upload a locally written partitioned Delta table to the
Azure emulator, then assert delta-explain's az:// path end to end
(baseline count, gate exit codes, degradation warning).

The fixture is written locally on purpose. This test validates
delta-explain's Azure read path, not deltalake's ability to write directly
to Azurite.

Expects: azurite reachable on 127.0.0.1:10000, DX_BIN pointing at the
delta-explain binary. Exits nonzero on any mismatch.
"""

import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile

import pyarrow as pa
from azure.storage.blob import BlobServiceClient
from deltalake import write_deltalake


ACCOUNT = "devstoreaccount1"

KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)

CONN = (
    f"DefaultEndpointsProtocol=http;"
    f"AccountName={ACCOUNT};"
    f"AccountKey={KEY};"
    f"BlobEndpoint=http://127.0.0.1:10000/{ACCOUNT};"
)

DX = os.environ.get(
    "DX_BIN",
    "delta-explain",
)

DX_OPTS = [
    "--option",
    f"account_name={ACCOUNT}",
    "--option",
    f"account_key={KEY}",
    "--option",
    "use_emulator=true",
    "--option",
    "allow_http=true",
]


def dx(args, expect_exit):
    proc = subprocess.run(
        [DX, *args],
        capture_output=True,
        text=True,
    )

    if proc.returncode != expect_exit:
        print(
            f"FAIL: {args} exited "
            f"{proc.returncode}, expected {expect_exit}"
        )

        print(
            proc.stdout[-2000:],
            proc.stderr[-2000:],
            sep="\n---\n",
        )

        sys.exit(1)

    return proc.stdout


def write_local_table(table_path):
    random.seed(7)

    countries = [
        "DE",
        "US",
        "IT",
    ]

    rows = [
        {
            "name": f"u{i}",
            "age": random.randint(18, 80),
            "country": random.choice(countries),
        }
        for i in range(600)
    ]

    def to_arrow(records):
        return pa.table(
            {
                key: [record[key] for record in records]
                for key in (
                    "name",
                    "age",
                    "country",
                )
            }
        )

    first = True

    for country in countries:
        country_rows = sorted(
            [
                row
                for row in rows
                if row["country"] == country
            ],
            key=lambda row: row["age"],
        )

        chunk = max(
            1,
            len(country_rows) // 4,
        )

        for offset in range(
            0,
            len(country_rows),
            chunk,
        ):
            write_deltalake(
                str(table_path),
                to_arrow(
                    country_rows[
                        offset : offset + chunk
                    ]
                ),
                partition_by=["country"],
                mode=(
                    "overwrite"
                    if first
                    else "append"
                ),
            )

            first = False


def upload_table(
    table_path,
    container,
    prefix,
):
    for path in sorted(
        table_path.rglob("*")
    ):
        if not path.is_file():
            continue

        relative = (
            path.relative_to(table_path)
            .as_posix()
        )

        blob_name = (
            f"{prefix}/{relative}"
        )

        with path.open("rb") as data:
            container.upload_blob(
                name=blob_name,
                data=data,
                overwrite=True,
            )


def main():
    service = (
        BlobServiceClient
        .from_connection_string(CONN)
    )

    container = (
        service.create_container(
            "lake"
        )
    )

    with tempfile.TemporaryDirectory() as tmp:
        table_path = (
            Path(tmp) / "users"
        )

        write_local_table(
            table_path
        )

        upload_table(
            table_path,
            container,
            "users",
        )

    print("table uploaded to azurite")

    out = dx(
        [
            "az://lake/users",
            *DX_OPTS,
            "--format",
            "json",
        ],
        0,
    )

    report = json.loads(out)

    assert (
        report["total_files"] > 0
    ), "empty snapshot"

    dx(
        [
            "az://lake/users",
            *DX_OPTS,
            "-w",
            "country = 'DE' AND age > 55",
            "--min-pruning",
            "50",
        ],
        0,
    )

    dx(
        [
            "az://lake/users",
            *DX_OPTS,
            "-w",
            "country = 'DE' AND age > 55",
            "--min-pruning",
            "99",
        ],
        1,
    )

    out = dx(
        [
            "az://lake/users",
            *DX_OPTS,
            "-w",
            (
                "country = 'DE' AND "
                "UPPER(name) = 'X'"
            ),
        ],
        0,
    )

    assert (
        "UNSUPPORTED_EXPRESSION"
        in out
    ), "degradation warning missing"

    print(
        "azurite smoke: OK "
        "(baseline, gates, degradation)"
    )


if __name__ == "__main__":
    main()