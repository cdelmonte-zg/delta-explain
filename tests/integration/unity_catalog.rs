//! `--catalog unity` against a fake Unity Catalog: a local HTTP server that
//! answers the two REST calls the resolver makes, with the table's storage
//! location pointing at a checked-in fixture. The vended AWS credentials
//! are ignored by the local object store, so the whole flow (name
//! resolution, credential vending, reading, provenance in the output) runs
//! inside `cargo test` without Docker or a cloud account.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use predicates::prelude::*;

use crate::common::{cmd, fixture};

const TABLE: &str = "lake.default.users";
const TOKEN: &str = "test-token";

#[derive(Debug, Clone)]
struct Request {
    method: String,
    path: String,
    authorization: Option<String>,
    body: String,
}

struct FakeCatalog {
    host: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

type Handler = dyn Fn(&Request) -> (u16, serde_json::Value) + Send + Sync + 'static;

impl FakeCatalog {
    fn spawn(
        handler: impl Fn(&Request) -> (u16, serde_json::Value) + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let requests: Arc<Mutex<Vec<Request>>> = Arc::default();
        let seen = Arc::clone(&requests);
        let handler: Arc<Handler> = Arc::new(handler);

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let request = read_request(&mut stream);
                let (status, body) = handler(&request);
                seen.lock().unwrap().push(request);
                let body = body.to_string();
                let reason = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    404 => "Not Found",
                    _ => "Error",
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });

        FakeCatalog { host, requests }
    }

    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    fn cmd(&self) -> assert_cmd::Command {
        let mut command = cmd();
        command
            .env("DATABRICKS_HOST", &self.host)
            .env("DATABRICKS_TOKEN", TOKEN)
            .args(["--catalog", "unity"]);
        command
    }
}

fn read_request(stream: &mut std::net::TcpStream) -> Request {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut authorization = None;
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).unwrap();
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            match name.to_ascii_lowercase().as_str() {
                "authorization" => authorization = Some(value.trim().to_string()),
                "content-length" => content_length = value.trim().parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).unwrap();

    Request {
        method,
        path,
        authorization,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

fn table_info(location: &str, format: &str) -> serde_json::Value {
    serde_json::json!({
        "name": "users",
        "catalog_name": "lake",
        "schema_name": "default",
        "table_type": "EXTERNAL",
        "data_source_format": format,
        "storage_location": location,
        "table_id": "0b9d5a0e-1c1c-4c3b-9a4e-7f0d6b2a1c11",
    })
}

fn aws_credentials() -> serde_json::Value {
    // The shape Unity Catalog OSS returns for statically configured
    // credentials: no expiration, the table's own url.
    serde_json::json!({
        "aws_temp_credentials": {
            "access_key_id": "AKIAFAKE",
            "secret_access_key": "secret",
            "session_token": "token",
        },
        "azure_user_delegation_sas": null,
        "gcp_oauth_token": null,
        "expiration_time": null,
        "url": "s3://cat/uc-external/users",
    })
}

fn uc_error(code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "error_code": code,
        "details": [{"reason": code, "@type": "google.rpc.ErrorInfo"}],
        "message": message,
    })
}

/// A catalog that knows `lake.default.users` as the `test-table` fixture
/// and vends AWS credentials for it.
fn working_catalog() -> FakeCatalog {
    let location = fixture("test-table");
    FakeCatalog::spawn(
        move |request| match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/api/2.1/unity-catalog/tables/lake.default.users") => {
                (200, table_info(&location, "DELTA"))
            }
            ("POST", "/api/2.1/unity-catalog/temporary-table-credentials") => {
                (200, aws_credentials())
            }
            (_, path) => (
                404,
                uc_error("TABLE_NOT_FOUND", &format!("Table not found: {path}")),
            ),
        },
    )
}

#[test]
fn name_is_resolved_and_read_with_vended_credentials() {
    let catalog = working_catalog();

    catalog
        .cmd()
        .args([TABLE, "-w", "country = 'DE'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Delta table: lake.default.users"))
        .stdout(predicate::str::contains("Resolved:    file://"))
        .stdout(predicate::str::contains("(unity catalog)"))
        .stdout(predicate::str::contains("Files in snapshot: 6"));

    let requests = catalog.requests();
    assert_eq!(
        requests.len(),
        2,
        "one lookup, one vending call: {requests:?}"
    );
    for request in &requests {
        assert_eq!(request.authorization.as_deref(), Some("Bearer test-token"));
    }
    let vending: serde_json::Value = serde_json::from_str(&requests[1].body).unwrap();
    assert_eq!(vending["table_id"], "0b9d5a0e-1c1c-4c3b-9a4e-7f0d6b2a1c11");
    assert_eq!(vending["operation"], "READ");
}

#[test]
fn json_carries_the_provenance_and_matches_the_direct_read() {
    let catalog = working_catalog();

    let output = catalog
        .cmd()
        .args([TABLE, "-w", "country = 'DE'", "--format", "json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let schema =
        std::fs::read_to_string(format!("{manifest_dir}/schemas/report-v0.6.schema.json")).unwrap();
    let schema: serde_json::Value = serde_json::from_str(&schema).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let violations: Vec<String> = validator
        .iter_errors(&json)
        .map(|e| e.to_string())
        .collect();
    assert!(
        violations.is_empty(),
        "schema violations:\n{}",
        violations.join("\n")
    );

    assert_eq!(json["table"], TABLE);
    assert_eq!(json["catalog"]["kind"], "unity");
    assert_eq!(json["catalog"]["name"], TABLE);
    let location = json["catalog"]["location"].as_str().unwrap();
    assert!(location.starts_with("file://"), "{location}");
    assert!(location.ends_with("/test-table/"), "{location}");

    let direct = cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "country = 'DE'",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let direct: serde_json::Value = serde_json::from_slice(&direct.stdout).unwrap();
    assert_eq!(json["total_files"], direct["total_files"]);
    assert_eq!(json["final_files"], direct["final_files"]);
    assert_eq!(json["phases"], direct["phases"]);
    assert!(direct["catalog"].is_null());
}

#[test]
fn unknown_table_keeps_the_catalog_message() {
    let catalog = FakeCatalog::spawn(|_| {
        (
            404,
            uc_error("TABLE_NOT_FOUND", "Table not found: lake.default.nope"),
        )
    });

    catalog
        .cmd()
        .args(["lake.default.nope"])
        .assert()
        .code(1)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("404"))
        .stderr(predicate::str::contains("TABLE_NOT_FOUND"))
        .stderr(predicate::str::contains(
            "Table not found: lake.default.nope",
        ));
}

#[test]
fn vending_failure_keeps_the_catalog_message() {
    let location = fixture("test-table");
    let catalog = FakeCatalog::spawn(move |request| match request.method.as_str() {
        "GET" => (200, table_info(&location, "DELTA")),
        _ => (
            400,
            uc_error("FAILED_PRECONDITION", "S3 bucket configuration not found."),
        ),
    });

    catalog
        .cmd()
        .args([TABLE])
        .assert()
        .code(1)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("400"))
        .stderr(predicate::str::contains(
            "S3 bucket configuration not found.",
        ));
}

#[test]
fn non_delta_table_is_refused_before_vending() {
    let location = fixture("test-table");
    let catalog = FakeCatalog::spawn(move |_| (200, table_info(&location, "PARQUET")));

    catalog
        .cmd()
        .args([TABLE])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("is not a Delta table"))
        .stderr(predicate::str::contains("PARQUET"));
    assert_eq!(
        catalog.requests().len(),
        1,
        "no vending call for a non-Delta table"
    );
}

#[test]
fn gcp_token_is_sent_as_bearer_to_the_object_store() {
    // The vended OAuth token goes in as the `bearer_token` option. A second
    // fake server plays GCS through `base_url` and records what the
    // kernel's first request carries.
    let gcs = FakeCatalog::spawn(|_| (404, serde_json::json!({"error": "no such object"})));
    let catalog = FakeCatalog::spawn(move |request| match request.method.as_str() {
        "GET" => (200, table_info("gs://lakebucket/users", "DELTA")),
        _ => (
            200,
            serde_json::json!({"gcp_oauth_token": {"oauth_token": "ya29.vended"}}),
        ),
    });

    catalog
        .cmd()
        .args([TABLE])
        .args(["--option", &format!("base_url={}", gcs.host)])
        .args(["--option", "allow_http=true"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("not yet supported").not());

    let requests = gcs.requests();
    let first = requests
        .iter()
        .find(|request| request.path.contains("_delta_log"))
        .unwrap_or_else(|| panic!("no log request reached the fake GCS: {requests:?}"));
    assert_eq!(first.authorization.as_deref(), Some("Bearer ya29.vended"));
    assert!(
        first.path.contains("lakebucket"),
        "bucket taken from the catalog location: {}",
        first.path
    );
}

#[test]
fn user_credential_sources_are_rejected_in_catalog_mode() {
    let catalog = working_catalog();

    for flag in ["--env-creds", "--public"] {
        catalog
            .cmd()
            .args([TABLE, flag])
            .assert()
            .code(1)
            .stderr(predicate::str::contains(
                "cannot be used with --catalog unity",
            ));
    }
    catalog
        .cmd()
        .args([TABLE, "--profile", "dev"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "cannot be used with --catalog unity",
        ));
}

#[test]
fn missing_host_and_malformed_name_fail_before_any_request() {
    let catalog = working_catalog();

    catalog
        .cmd()
        .env_remove("DATABRICKS_HOST")
        .args([TABLE])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("DATABRICKS_HOST is required"));

    catalog
        .cmd()
        .args(["users"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("expected catalog.schema.table"));

    assert!(catalog.requests().is_empty());
}
