//! The single integration-test binary. One module per feature area; shared
//! helpers (binary invocation, fixture paths, synthetic log builder) live in
//! `common`. CLI-surface concerns (arguments, exit codes, output shape) stay
//! in `cli`; the semantics of a feature belong in that feature's module.

mod common;

mod aws_profile;
mod checkpoint_partition_columns;
mod checkpoint_stats;
mod cli;
mod debug_ir;
mod exotic_logs;
mod json_contract;
mod nested_stats;
mod partial_stats;
mod partition_columns;
mod protocol_features;
mod quickstart;
mod semantic_regression;
mod stats_budget;
mod synthetic_log;
mod temporal_coercions;
mod time_travel;
