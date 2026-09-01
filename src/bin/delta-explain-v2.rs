use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;

use delta_explain::v2::error::Result;
use delta_explain::v2::execution::{self, ExecutionInput};
use delta_explain::v2::gates::GateConfig;
use delta_explain::v2::render;
use delta_explain::v2::table;
use delta_explain::v2::table_uri;

#[derive(Parser)]
struct Cli {
    path: String,

    #[arg(short = 'w', long = "where")]
    predicate: Option<String>,

    #[arg(long = "explain-why")]
    explain_why: bool,

    /// Fail if total pruning is below this percentage.
    #[arg(long, value_name = "PERCENT", requires = "predicate")]
    min_pruning: Option<f64>,

    /// Fail if any file in the snapshot is missing statistics.
    #[arg(long)]
    assert_stats: bool,
}

fn main() -> ExitCode {
    match try_main() {
        Ok(exit_code) => exit_code,

        Err(err) => {
            eprintln!("Error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn try_main() -> Result<ExitCode> {
    let cli = Cli::parse();

    let table_url = table_uri::parse(&cli.path)?;

    let options = HashMap::<String, String>::new();

    let store = store_from_url_opts(&table_url, options)?;

    let engine = DefaultEngineBuilder::new(store.clone()).build();

    let table = table::open(&table_url, &store, &engine)?;

    let result = execution::execute(
        ExecutionInput {
            table_path: &cli.path,
            predicate: cli.predicate.as_deref(),

            gate_config: GateConfig {
                min_pruning: cli.min_pruning,
                assert_stats: cli.assert_stats,
            },
        },
        &table,
        &engine,
    )?;

    for failure in render::gate_failures(&result.gates) {
        eprintln!("{failure}");
    }

    print!("{}", render::text(&result.report, cli.explain_why,));

    let exit_code = if result.gates.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    };

    Ok(exit_code)
}
