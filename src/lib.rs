//! delta-explain: make Delta Lake file pruning visible.
//!
//! This library crate hosts the analysis machinery behind the `delta-explain`
//! CLI: predicate classification and lowering, kernel-backed scanning and
//! statistics, and report rendering. The binary in `main.rs` is the CLI layer
//! on top.

pub mod predicate_analyzer;
pub mod predicate_parser;
pub mod report;
pub mod scan;
pub mod stats;
