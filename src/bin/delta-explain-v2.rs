use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;

use delta_explain::v2::analysis;
use delta_explain::v2::error::Result;
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

    println!("version: {}", table.snapshot.version());

    println!("files: {}", table.metadata.baseline.files.len());

    println!("files with stats: {}", table.metadata.baseline.stats.len());

    println!("partition columns: {:?}", table.metadata.partition_columns);

    if let Some(predicate) = cli.predicate.as_deref() {
        let result = analysis::analyze(predicate, &table, &engine)?;

        println!("confidence: {:?}", analysis::confidence(&result));

        println!("partition-safe:");
        for pred in &result.classification.partition_safe {
            println!("  {pred}");
        }

        println!("partition-exact:");
        for pred in &result.classification.partition_exact {
            println!("  {pred}");
        }

        println!("stats-safe:");
        for pred in &result.classification.stats_safe {
            println!("  {pred}");
        }

        println!("unsplittable:");
        for fragment in &result.classification.unsplittable {
            println!("  {:?}: {}", fragment.handling, fragment.predicate);
        }

        match &result.partition.survivors {
            Some(files) => {
                println!(
                    "partition survivors: {} / {}",
                    files.len(),
                    table.metadata.baseline.files.len()
                );
            }

            None => {
                println!("partition survivors: n/a");
            }
        }

        println!(
            "partition evaluation gaps: {}",
            result.partition.evaluation_gaps
        );

        match &result.scan.survivors {
            Some(files) => {
                println!(
                    "scan survivors: {} / {}",
                    files.len(),
                    table.metadata.baseline.files.len()
                );
            }

            None => {
                println!("scan survivors: n/a");
            }
        }
    }

    Ok(())
}
