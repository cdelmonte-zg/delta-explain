use std::process::ExitCode;

use clap::Parser;

use delta_explain::v2::error::Result;
use delta_explain::v2::execution::{self, ExecutionInput};
use delta_explain::v2::gates::GateConfig;
use delta_explain::v2::instrumentation::{Instrumentation, NoOpInstrumentation};
use delta_explain::v2::presentation::{self, OutputFormat, PresentationOptions};
use delta_explain::v2::storage::{self, StorageConfig, StorageOption};
use delta_explain::v2::table;
use delta_explain::v2::table_uri;

#[cfg(feature = "debug-ir")]
use delta_explain::v2::instrumentation::DebugIrObserver;

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

    #[cfg(feature = "debug-ir")]
    #[arg(long = "debug-ir", value_name = "FILE")]
    debug_ir: Option<String>,

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

    /// Analyze this table version instead of the latest.
    #[arg(long, value_name = "N")]
    at_version: Option<u64>,

    /// AWS region (S3 only).
    #[arg(long)]
    region: Option<String>,

    /// Key=value options for the object-store backend.
    /// Can be repeated.
    #[arg(long = "option", value_name = "KEY=VALUE")]
    options: Vec<String>,

    /// Get cloud credentials from environment variables.
    #[arg(long)]
    env_creds: bool,

    /// Resolve static AWS credentials from this profile.
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,

    /// Access a public bucket.
    #[arg(long)]
    public: bool,
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

    let custom_options = cli
        .options
        .iter()
        .map(|raw| raw.parse::<StorageOption>())
        .collect::<Result<Vec<_>>>()?;

    let runtime = storage::open(
        &table_url,
        &StorageConfig {
            env_credentials: cli.env_creds,

            profile: cli.profile.clone(),

            region: cli.region.clone(),

            public: cli.public,

            options: custom_options,
        },
    )?;

    let mut instrumentation = build_instrumentation(&cli)?;

    instrumentation.invocation(&cli.path, cli.predicate.as_deref())?;

    let table = table::open(
        &table_url,
        &runtime.store,
        runtime.engine.as_ref(),
        table::OpenOptions {
            version: cli.at_version,
        },
        instrumentation.as_mut(),
    )?;

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
        runtime.engine.as_ref(),
        instrumentation.as_mut(),
    )?;

    let elapsed_ms = start.elapsed().as_millis();

    // All table/kernel/analysis work is complete.
    // Close instrumentation before presentation/rendering
    // so the debug dump is complete even if output later
    // fails or a gate returns a failing exit status.
    instrumentation.finish()?;

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

fn build_instrumentation(cli: &Cli) -> Result<Box<dyn Instrumentation>> {
    #[cfg(feature = "debug-ir")]
    if let Some(path) = cli.debug_ir.as_deref() {
        return Ok(Box::new(DebugIrObserver::create(path)?));
    }

    #[cfg(not(feature = "debug-ir"))]
    let _ = cli;

    Ok(Box::new(NoOpInstrumentation))
}
