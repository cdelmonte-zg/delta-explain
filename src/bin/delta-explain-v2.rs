use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_explain::v2::error::Result;
use delta_explain::v2::table;
use delta_explain::v2::table_uri;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;

#[derive(Parser)]
struct Cli {
    path: String,
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

    Ok(())
}
