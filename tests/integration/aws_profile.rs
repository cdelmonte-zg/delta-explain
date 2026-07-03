//! Integration tests for `--profile`: static AWS credentials resolved from
//! the standard shared files, pointed at temp fixtures via the
//! `AWS_SHARED_CREDENTIALS_FILE` / `AWS_CONFIG_FILE` overrides the AWS CLI
//! also honors. No real S3 endpoint is needed: resolution failures must be
//! clean errors, and successful resolution is observable because the run
//! proceeds to the (unreachable) endpoint instead of failing on credentials.

use crate::common::cmd;
use std::io::Write;

use predicates::prelude::*;

fn write_temp(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    path
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("dx-aws-profile-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn missing_profile_fails_cleanly() {
    let dir = temp_dir("missing");
    let creds = write_temp(
        &dir,
        "credentials",
        "[other]\naws_access_key_id = X\naws_secret_access_key = Y\n",
    );
    let config = write_temp(&dir, "config", "");
    cmd()
        .env("AWS_SHARED_CREDENTIALS_FILE", &creds)
        .env("AWS_CONFIG_FILE", &config)
        .args(["s3://bucket/table", "--profile", "nope"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("profile 'nope' not found"));
}

#[test]
fn sso_profile_fails_with_actionable_message() {
    let dir = temp_dir("sso");
    let creds = write_temp(&dir, "credentials", "");
    let config = write_temp(
        &dir,
        "config",
        "[profile corp]\nsso_start_url = https://corp.awsapps.com/start\nregion = eu-central-1\n",
    );
    cmd()
        .env("AWS_SHARED_CREDENTIALS_FILE", &creds)
        .env("AWS_CONFIG_FILE", &config)
        .args(["s3://bucket/table", "--profile", "corp"])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("sso_start_url")
                .and(predicate::str::contains("aws configure export-credentials")),
        );
}

#[test]
fn static_profile_resolves_and_proceeds_past_credentials() {
    let dir = temp_dir("static");
    let creds = write_temp(
        &dir,
        "credentials",
        "[dev]\naws_access_key_id = AKIADEV\naws_secret_access_key = devsecret\n",
    );
    let config = write_temp(&dir, "config", "[profile dev]\nregion = eu-central-1\n");
    // Credentials resolve, so the failure is the unreachable endpoint (or a
    // kernel storage error), NOT a credentials error.
    cmd()
        .env("AWS_SHARED_CREDENTIALS_FILE", &creds)
        .env("AWS_CONFIG_FILE", &config)
        .args([
            "s3://this-bucket-does-not-exist-dx/table",
            "--profile",
            "dev",
            "--option",
            "aws_endpoint=http://127.0.0.1:1",
            "--option",
            "aws_allow_http=true",
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("profile")
                .not()
                .and(predicate::str::contains("aws_access_key_id").not()),
        );
}
