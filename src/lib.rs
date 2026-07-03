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

pub mod attribution;
pub mod credentials;
pub mod error;
pub mod gates;
pub mod kernel_bridge;
pub mod predicate_analyzer;
pub mod predicate_ast;
pub mod render;
pub mod report;
pub mod scan;
pub mod stats;
