#!/usr/bin/env python3
"""Catalog resolution probe: name -> storage location -> delta-explain.

Three phases, each skippable:

1. Spark writes the tables (docker exec into the stack's Spark container).
   The MANAGED Unity Catalog table gets its location from the catalog
   first (POST /staging-tables): the server picks a path under its
   storage root, Spark writes there, and the registration in phase 2
   names that path. This is how managed tables are born in Unity Catalog;
   a client-chosen path is refused.
2. Unity Catalog registration through its REST API: catalog `lake`, schema
   `default`, an EXTERNAL table over the path Spark wrote and the MANAGED
   table over its staging location.
3. Resolution: ask each catalog for the table's location and run
   delta-explain against it. The Hive location comes from the metastore
   through Spark's Hive client (phase 1); the Unity Catalog locations from
   GET /tables/{full_name}.

Requires the stack from docker-compose.yml and delta-explain on PATH (or
DX_BIN=/path/to/binary). DX_CAT_FRESH=1 forces the tables to be rewritten.
"""
import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORK = HERE / "work"


def load_env():
    """The optional .env docker compose reads for the stack; the host side
    needs the *_TABLE_PATH entries from it too. Shell exports win."""
    env_file = HERE / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_env()
DX_BIN = os.environ.get("DX_BIN", "delta-explain")
CONTAINER = "dxcat-spark"
UC_HOST = os.environ.get("UC_HOST", "http://localhost:8080")
UC = f"{UC_HOST}/api/2.1/unity-catalog"
# How to reach RustFS from the host; credentials are a separate matter.
RUSTFS_TRANSPORT = [
    "--option", "aws_endpoint=http://localhost:9020",
    "--option", "aws_allow_http=true",
    "--option", "aws_virtual_hosted_style_request=false",
    "--option", "aws_region=us-east-1",
]
DX_OPTIONS = [
    *RUSTFS_TRANSPORT,
    "--option", "aws_access_key_id=rustfsadmin",
    "--option", "aws_secret_access_key=rustfsadmin",
]
PREDICATE = "country = 'DE'"

UC_CATALOG = "lake"
UC_SCHEMA = "default"
UC_TABLES = {
    # name -> (table_type, key in locations.json)
    "users_ext": ("EXTERNAL", "uc_external"),
    "users_mgd": ("MANAGED", "uc_managed"),
}

# Optional: Delta tables in a real ADLS Gen2 account and a real GCS bucket
# (AZURE_TABLE_PATH, GCS_TABLE_PATH in .env), registered EXTERNAL and read
# only with what the catalog vends. The schema is the one of the validation
# demo table (`lake/users`): the host has no cloud credentials of its own
# to read the log with.
CLOUD_TABLES = {
    name: path for name, path in (
        ("users_azure", os.environ.get("AZURE_TABLE_PATH")),
        ("users_gcs", os.environ.get("GCS_TABLE_PATH")),
    ) if path
}
DEMO_SCHEMA = [
    {"name": "name", "type": "string", "nullable": True, "metadata": {}},
    {"name": "age", "type": "long", "nullable": True, "metadata": {}},
    {"name": "country", "type": "string", "nullable": True, "metadata": {}},
]

# Spark type name -> Unity Catalog ColumnTypeName.
UC_TYPES = {
    "long": "LONG", "integer": "INT", "string": "STRING", "double": "DOUBLE",
}


def wait_for_stack(timeout=180):
    """Both catalogs take a few seconds after `up`; poll them instead of
    failing the first REST call or the first Spark connection."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            uc_ok = uc_request("GET", "/catalogs")[0] == 200
        except (urllib.error.URLError, ConnectionError):
            uc_ok = False
        try:
            with socket.create_connection(("localhost", 9083), timeout=2):
                hms_ok = True
        except OSError:
            hms_ok = False
        if uc_ok and hms_ok:
            return
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"stack not ready after {timeout}s "
                f"(unity catalog: {uc_ok}, metastore: {hms_ok})")
        time.sleep(2)


def uc_managed_location():
    """Where the MANAGED table's data must go: the registered table's
    location if it exists, else a fresh staging location from the catalog.
    Travels to the Spark container as a file on the shared volume."""
    full_name = f"{UC_CATALOG}.{UC_SCHEMA}.users_mgd"
    status, payload = uc_request("GET", f"/tables/{full_name}")
    if status == 200:
        location = payload["storage_location"]
        staging_id = payload["table_id"]
        print(f"Unity Catalog: {full_name} exists at {location}")
    else:
        uc_create("/catalogs", {"name": UC_CATALOG}, f"catalog {UC_CATALOG}")
        uc_create("/schemas", {"name": UC_SCHEMA, "catalog_name": UC_CATALOG},
                  f"schema {UC_CATALOG}.{UC_SCHEMA}")
        status, payload = uc_request("POST", "/staging-tables", {
            "name": "users_mgd",
            "catalog_name": UC_CATALOG,
            "schema_name": UC_SCHEMA,
        })
        if status != 200:
            raise RuntimeError(f"staging table: {status} {payload}")
        location = payload["staging_location"]
        staging_id = payload["id"]
        print(f"Unity Catalog: staging location for {full_name}: {location}")
    with open(WORK / "uc_staging.json", "w") as f:
        json.dump({"uc_managed_location": location, "staging_id": staging_id}, f)
    return location


def staging_id():
    with open(WORK / "uc_staging.json") as f:
        return json.load(f)["staging_id"]


def write_tables():
    uc_managed_location()
    fresh = os.environ.get("DX_CAT_FRESH", "")
    subprocess.run(
        [
            "docker", "exec", "-e", f"DX_CAT_FRESH={fresh}",
            CONTAINER, "bash", "-lc",
            "cd /home/jovyan/work && $SPARK_HOME/bin/spark-submit "
            "--packages io.delta:delta-spark_2.13:4.3.0,"
            "org.apache.hadoop:hadoop-aws:3.4.2 "
            "catalog_tables.py",
        ],
        check=True,
    )


def locations():
    with open(WORK / "locations.json") as f:
        return json.load(f)


def uc_request(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{UC}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        payload = e.read().decode(errors="replace")
        try:
            payload = json.loads(payload)
        except ValueError:
            pass
        return e.code, payload


def uc_create(path, body, what):
    status, payload = uc_request("POST", path, body)
    if status == 200:
        print(f"  created {what}")
    elif (isinstance(payload, dict)
          and str(payload.get("error_code", "")).endswith("ALREADY_EXISTS")):
        print(f"  {what} already exists")
    else:
        raise RuntimeError(f"creating {what} failed: {status} {payload}")


def uc_columns(schema_fields, partition_columns):
    cols = []
    for pos, field in enumerate(schema_fields):
        spark_type = field["type"]
        col = {
            "name": field["name"],
            "type_text": spark_type,
            "type_json": json.dumps(field),
            "type_name": UC_TYPES[spark_type],
            "position": pos,
            "nullable": field.get("nullable", True),
        }
        if field["name"] in partition_columns:
            col["partition_index"] = partition_columns.index(field["name"])
        cols.append(col)
    return cols


def register_uc(locs):
    print("Unity Catalog registration")
    uc_create("/catalogs", {"name": UC_CATALOG}, f"catalog {UC_CATALOG}")
    uc_create("/schemas", {"name": UC_SCHEMA, "catalog_name": UC_CATALOG},
              f"schema {UC_CATALOG}.{UC_SCHEMA}")
    columns = uc_columns(locs["schema"], ["country"])
    for name, (table_type, key) in UC_TABLES.items():
        # The catalog stores the S3 URL; the s3a:// scheme is Hadoop's.
        location = locs[key]["location"].replace("s3a://", "s3://", 1)
        body = {
            "name": name,
            "catalog_name": UC_CATALOG,
            "schema_name": UC_SCHEMA,
            "table_type": table_type,
            "data_source_format": "DELTA",
            "columns": columns,
            "storage_location": location,
        }
        if table_type == "MANAGED":
            # The server ties a MANAGED table to the staging table that
            # issued its location: same id, same path, or it refuses.
            body["properties"] = {"io.unitycatalog.tableId": staging_id()}
        uc_create("/tables", body, f"{table_type} table {name} at {location}")
    for name, path in CLOUD_TABLES.items():
        uc_create("/tables", {
            "name": name,
            "catalog_name": UC_CATALOG,
            "schema_name": UC_SCHEMA,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": uc_columns(DEMO_SCHEMA, ["country"]),
            "storage_location": path,
        }, f"EXTERNAL table {name} at {path}")


def uc_resolve(full_name):
    status, payload = uc_request("GET", f"/tables/{full_name}")
    if status != 200:
        raise RuntimeError(f"GET /tables/{full_name}: {status} {payload}")
    return payload


def uc_vend(table_id):
    """Temporary READ credentials for the table: the key, secret and session
    token the server was configured with at start (RustFS's own, see
    uc/bootstrap.sh), scoped by nothing but their 12-hour lifetime."""
    status, payload = uc_request("POST", "/temporary-table-credentials",
                                 {"table_id": table_id, "operation": "READ"})
    if status != 200:
        raise RuntimeError(f"POST /temporary-table-credentials: {status} {payload}")
    return payload


def vended_options(credentials):
    """The vended credentials as `--option` keys, or None when object_store
    has no key for them: a GCS OAuth token only travels through
    `--catalog unity`, where it is installed as a credential provider."""
    if credentials.get("gcp_oauth_token"):
        return None
    if credentials.get("azure_user_delegation_sas"):
        sas = credentials["azure_user_delegation_sas"]["sas_token"]
        return ["--option", f"azure_storage_sas_key={sas}"]
    aws = credentials["aws_temp_credentials"]
    return [
        *RUSTFS_TRANSPORT,
        "--option", f"aws_access_key_id={aws['access_key_id']}",
        "--option", f"aws_secret_access_key={aws['secret_access_key']}",
        "--option", f"aws_session_token={aws['session_token']}",
    ]


def describe_vended(credentials):
    expires = credentials.get("expiration_time")
    expires = ("unreported" if expires is None else time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires / 1000)))
    if credentials.get("azure_user_delegation_sas"):
        return f"azure_user_delegation_sas, expiration {expires}"
    if credentials.get("gcp_oauth_token"):
        return f"gcp_oauth_token, expiration {expires}"
    key = credentials["aws_temp_credentials"]["access_key_id"]
    return f"aws_temp_credentials, access key {key}, expiration {expires}"


def explain(uri, options=DX_OPTIONS, env=None):
    proc = subprocess.run(
        [DX_BIN, uri, *options, "-w", PREDICATE, "--format", "json"],
        capture_output=True, text=True, env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"delta-explain failed on {uri}:\n{proc.stderr}")
    report = json.loads(proc.stdout)
    return report["total_files"], report["final_files"], report["total_pruning_pct"]


def explain_by_name(full_name, location):
    """`--catalog unity`: the resolver does the two REST calls above and
    reads with what the catalog vends. The server has authorization
    disabled, so any token satisfies the CLI's requirement. The RustFS
    transport is user-controlled in catalog mode; other backends drop
    keys they do not own."""
    env = {**os.environ, "DATABRICKS_HOST": UC_HOST, "DATABRICKS_TOKEN": "unused"}
    options = RUSTFS_TRANSPORT if location.startswith("s3://") else []
    return explain(full_name, ["--catalog", "unity", *options], env)


def to_delta_explain_uri(location):
    # Both catalogs hand back what the writer stored: s3a:// from Spark and
    # the metastore, s3:// from the Unity Catalog registration. delta-explain
    # speaks object_store URLs, so a resolver has to normalize the scheme.
    return location.replace("s3a://", "s3://", 1)


def resolve(locs):
    results = []
    hive = locs["hive"]
    print(f"Hive Metastore: {hive['reference']}")
    print(f"  location (via Spark's Hive client): {hive['location']}")
    print(f"  database location: {hive['database_location']}")
    print(f"  format: {hive['format']}, files: {hive['num_files']}, "
          f"partitioned by {hive['partition_columns']}")
    results.append((hive["reference"], hive["location"]))

    vended = {}
    uc_names = list(UC_TABLES) + list(CLOUD_TABLES)
    for name in uc_names:
        full_name = f"{UC_CATALOG}.{UC_SCHEMA}.{name}"
        info = uc_resolve(full_name)
        print(f"Unity Catalog: {full_name}")
        for key in ("table_type", "data_source_format", "storage_location",
                    "table_id"):
            print(f"  {key}: {info.get(key)}")
        # The static S3 generator reports no expiration (null): the
        # lifetime is the one RustFS gave the token at bootstrap.
        credentials = uc_vend(info["table_id"])
        print(f"  vended: {describe_vended(credentials)}")
        results.append((full_name, info["storage_location"]))
        vended[full_name] = credentials

    print(f"\ndelta-explain on the resolved locations, -w \"{PREDICATE}\"")
    for reference, location in results:
        if location.startswith(("abfss://", "gs://")):
            continue  # the host holds no cloud credentials: vended only
        uri = to_delta_explain_uri(location)
        total, kept, pct = explain(uri)
        print(f"  {reference:<26} {uri}")
        print(f"  {'':<26} {total} files, {kept} kept, {pct:.1f}% pruned")

    print("\nsame, with the credentials Unity Catalog vended, as --option")
    for reference, location in results:
        if reference not in vended:
            continue
        options = vended_options(vended[reference])
        if options is None:
            print(f"  {reference:<26} no --option key for a GCS OAuth token")
            continue
        uri = to_delta_explain_uri(location)
        total, kept, pct = explain(uri, options)
        print(f"  {reference:<26} {total} files, {kept} kept, {pct:.1f}% pruned")

    print("\nsame, by name through --catalog unity")
    for reference, location in results:
        if reference not in vended:
            continue
        total, kept, pct = explain_by_name(reference, location)
        print(f"  {reference:<26} {total} files, {kept} kept, {pct:.1f}% pruned")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--skip-write", action="store_true",
                        help="reuse work/locations.json, do not run Spark")
    parser.add_argument("--skip-register", action="store_true",
                        help="do not (re)register the Unity Catalog tables")
    args = parser.parse_args()
    wait_for_stack()
    if not args.skip_write:
        write_tables()
    locs = locations()
    if not args.skip_register:
        register_uc(locs)
    resolve(locs)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
