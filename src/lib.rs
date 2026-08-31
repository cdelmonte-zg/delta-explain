//! delta-explain: make Delta Lake file pruning visible.
//!
//! This library crate hosts the analysis machinery behind the `delta-explain`
//! CLI: predicate classification and lowering, kernel-backed scanning and
//! statistics, and report rendering. The binary in `main.rs` is the CLI layer
//! on top.
//!
//! **API stability**: the Rust API is currently internal and unstable. It
//! exists to serve the CLI and its tests; module structure and signatures may
//! change between releases without semver guarantees. The CLI surface and the
//! versioned JSON output schema are the stable contracts.

// The project rule "no unwrap/expect/panic/unreachable in production code"
// enforced by the compiler instead of review discipline. Unit tests compile
// with cfg(test), so the lints stay off there; integration tests are a
// separate crate and are unaffected.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod attribution;
pub mod credentials;
#[cfg(feature = "debug-ir")]
pub mod debug_dump;
pub mod diagnostics;
pub mod error;
pub mod features;
pub mod gates;
pub mod kernel_bridge;
pub mod partition_eval;
pub mod predicate_analyzer;
pub mod predicate_ast;
pub mod render;
pub mod report;
pub mod scan;
pub mod stats;
pub mod v2;
