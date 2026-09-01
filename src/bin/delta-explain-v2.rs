use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;

use delta_explain::v2::error::Result;
use delta_explain::v2::execution::{self, ExecutionInput};
use delta_explain::v2::gates::GateConfig;
use delta_explain::v2::presentation::{self, OutputFormat, PresentationOptions};
use delta_explain::v2::table;
use delta_explain::v2::table_uri;

#[derive(Parser)]
struct Cli {
    path: String,

    #[arg(short = 'w', long = "where")]
    predicate: Option<String>,

    #[arg(short, long)]
    verbose: bool,

    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    #[arg(long = "explain-why")]
    explain_why: bool,

    /// Output format.
    #[arg(
        long,
        default_value = "text",
        value_parser = ["text", "json"]
    )]
    format: String,

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

    let start = std::time::Instant::now();

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

    let elapsed_ms = start.elapsed().as_millis();

    let presentation = presentation::build(
        &result.report,
        &result.gates,
        &table.metadata.baseline,
        elapsed_ms,
        PresentationOptions {
            verbose: cli.verbose,

            limit: cli.limit,

            explain_why: cli.explain_why,
        },
    );

    for failure in presentation::gate_failures(&presentation) {
        eprintln!("{failure}");
    }

    let format = match cli.format.as_str() {
        "json" => OutputFormat::Json,

        _ => OutputFormat::Text,
    };

    let output = format.render(&presentation)?;

    print!("{output}");

    let exit_code = if result.gates.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    };

    Ok(exit_code)
}
