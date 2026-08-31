use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;

use delta_explain::v2::analysis;
use delta_explain::v2::error::Result;
use delta_explain::v2::report;
use delta_explain::v2::table;
use delta_explain::v2::table_uri;

#[derive(Parser)]
struct Cli {
    path: String,

    #[arg(short = 'w', long = "where")]
    predicate: Option<String>,
}

fn main() -> ExitCode {
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,

        Err(err) => {
            eprintln!("Error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn try_main() -> Result<()> {
    let cli = Cli::parse();

    let table_url = table_uri::parse(&cli.path)?;

    let options = HashMap::<String, String>::new();

    let store = store_from_url_opts(&table_url, options)?;

    let engine = DefaultEngineBuilder::new(store.clone()).build();

    let table = table::open(&table_url, &store, &engine)?;

    let analysis_result = match cli.predicate.as_deref() {
        Some(predicate) => Some(analysis::analyze(predicate, &table, &engine)?),

        None => None,
    };

    let report = report::build(cli.predicate.as_deref(), &table, analysis_result.as_ref());

    println!("version: {}", report.table.version);

    println!("files: {}", report.table.total_files);

    println!("files with stats: {}", report.table.files_with_stats);

    println!("partition columns: {:?}", report.table.partition_columns);

    if let Some(predicate) = &report.predicate {
        println!("confidence: {:?}", predicate.confidence);

        println!("partition-safe:");

        for pred in &predicate.classification.partition_safe {
            println!("  {pred}");
        }

        println!("partition-exact:");

        for pred in &predicate.classification.partition_exact {
            println!("  {pred}");
        }

        println!("stats-safe:");

        for pred in &predicate.classification.stats_safe {
            println!("  {pred}");
        }

        println!("unsplittable:");

        for fragment in &predicate.classification.unsplittable {
            println!("  {:?}: {}", fragment.handling, fragment.predicate);
        }

        println!(
            "partition evaluation gaps: {}",
            predicate.partition_evaluation_gaps
        );

        println!("phases:");

        for phase in &predicate.phases {
            println!(
                "  {:?}: {} -> {} [{:?}]",
                phase.kind, phase.input_count, phase.output_count, phase.confidence
            );
        }
    }

    Ok(())
}
