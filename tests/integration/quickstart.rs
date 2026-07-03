//! The quickstart script is sold as repeatable; this smoke keeps that
//! claim tested. It runs the real script against the binary under test
//! and asserts the five beats all reached their expected outcome.

// The quickstart is a POSIX shell script; on Windows runners bash exists
// only via Git Bash and path translation makes the smoke flaky, so the
// script's guarantee is tested where it is meant to run.
#[cfg(unix)]
#[test]
fn quickstart_script_runs_end_to_end() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let script = format!("{manifest_dir}/examples/quickstart/quickstart.sh");
    let output = std::process::Command::new("bash")
        .arg(&script)
        .env("DX_BIN", env!("CARGO_BIN_EXE_delta-explain"))
        .output()
        .expect("quickstart script runs");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "quickstart failed:\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for marker in [
        "===== 1.",
        "===== 2.",
        "===== 3.",
        "===== 4.",
        "===== 5.",
        "UNSUPPORTED_EXPRESSION",
        "\"result\": \"fail\"",
        "DELETION_VECTORS",
        "Done.",
    ] {
        assert!(stdout.contains(marker), "missing marker {marker:?}");
    }
}
