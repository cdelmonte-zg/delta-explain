use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;

use delta_explain::v2::analysis;
use delta_explain::v2::error::Result;
use delta_explain::v2::render;
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

    let report = report::build(
        &cli.path,
        cli.predicate.as_deref(),
        &table,
        analysis_result.as_ref(),
    );

    print!("{}", render::text(&report));

    Ok(())
}
