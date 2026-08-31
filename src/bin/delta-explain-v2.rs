use std::collections::HashMap;
use std::process::ExitCode;

use clap::Parser;
use delta_kernel::{Snapshot};
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;
use url::Url;

use delta_explain::v2::error::{Error, Result};
use delta_explain::v2::metadata::scan::scan_baseline;

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

    let url = parse_table_uri(&cli.path)?;

    let options = HashMap::<String, String>::new();
    let store = store_from_url_opts(&url, options)?;
    let engine = DefaultEngineBuilder::new(store).build();

    let snapshot = Snapshot::builder_for(url).build(&engine)?;

    let baseline = scan_baseline(snapshot.clone(), &engine)?;

    println!("version: {}", snapshot.version());
    println!("files: {}", baseline.files.len());
    println!("files with stats: {}", baseline.stats.len());

    Ok(())
}

fn parse_table_uri(path: &str) -> Result<Url> {
    if let Ok(mut url) = Url::parse(path)
        && url.scheme() != "file"
        && url.has_host()
    {
        if !url.path().ends_with('/') {
            let with_slash = format!("{}/", url.path());
            url.set_path(&with_slash);
        }

        return Ok(url);
    }

    let absolute = std::fs::canonicalize(path)
        .map_err(|e| Error::TableUri(format!("Invalid path '{path}': {e}")))?;

    Url::from_directory_path(&absolute)
        .map_err(|_| Error::TableUri(format!("Cannot convert path to URL: {absolute:?}")))
}