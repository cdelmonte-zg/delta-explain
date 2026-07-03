//! Generates the synthetic Delta logs behind the README's performance
//! numbers, so anyone can reproduce them. delta-explain never reads parquet
//! data files, so the log alone is a complete table for its purposes.
//!
//! Three shapes, from least to most production-like:
//!
//! ```bash
//! # one commit with 200k adds (the simplest shape)
//! cargo run --release --example gen_scale_log -- /tmp/scale-single --files 200000
//!
//! # the same files across 2000 commits (listing and replay overhead)
//! cargo run --release --example gen_scale_log -- /tmp/scale-commits --files 200000 --commits 2000
//!
//! # plus a real kernel-written checkpoint over those commits
//! cargo run --release --example gen_scale_log -- /tmp/scale-ckpt --files 200000 --commits 2000 --checkpoint
//! ```
//!
//! Layout: partitioned by `country` (200 values), an `age` int column whose
//! per-file ranges tile [i%100, i%100+10], so predicates have predictable
//! selectivity at any size. Timing procedure: /usr/bin/time -v on a release
//! binary, baseline and with a predicate; the numbers live in the README's
//! Performance notes.

use std::fmt::Write as _;
use std::io::Write as _;

use clap::Parser;

#[derive(Parser)]
struct Args {
    /// Output directory for the table (created; must not already contain
    /// a _delta_log)
    dir: std::path::PathBuf,

    /// Number of add actions (files) in the snapshot
    #[arg(long, default_value_t = 200_000)]
    files: usize,

    /// Number of commits to spread the files across
    #[arg(long, default_value_t = 1)]
    commits: usize,

    /// Write a kernel-produced checkpoint at the final version
    #[arg(long)]
    checkpoint: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let log_dir = args.dir.join("_delta_log");
    if log_dir.exists() {
        return Err(format!(
            "{} already exists, refusing to overwrite",
            log_dir.display()
        )
        .into());
    }
    std::fs::create_dir_all(&log_dir)?;

    let schema = serde_json::json!({
        "type": "struct",
        "fields": [
            {"name": "country", "type": "string", "nullable": true, "metadata": {}},
            {"name": "age", "type": "integer", "nullable": true, "metadata": {}},
        ]
    });
    let protocol = serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = serde_json::json!({"metaData": {
        "id": "00000000-0000-0000-0000-00000000bench",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": schema.to_string(),
        "partitionColumns": ["country"],
        "configuration": {},
        "createdTime": 1_750_000_000_000_u64,
    }});

    let commits = args.commits.max(1);
    let per_commit = args.files.div_ceil(commits);
    let mut written = 0usize;

    for version in 0..commits {
        let mut content = String::new();
        if version == 0 {
            writeln!(content, "{protocol}")?;
            writeln!(content, "{metadata}")?;
        }
        for _ in 0..per_commit {
            if written >= args.files {
                break;
            }
            let i = written;
            let country = format!("C{:03}", i % 200);
            let lo = i % 100;
            let stats = format!(
                "{{\"numRecords\":10000,\"minValues\":{{\"age\":{lo}}},\
                 \"maxValues\":{{\"age\":{}}},\"nullCount\":{{\"age\":0}}}}",
                lo + 10
            );
            let add = serde_json::json!({"add": {
                "path": format!("country={country}/part-{i:07}.parquet"),
                "partitionValues": {"country": country},
                "size": 52_428_800_u64,
                "modificationTime": 1_750_000_000_000_u64 + i as u64,
                "dataChange": true,
                "stats": stats,
            }});
            writeln!(content, "{add}")?;
            written += 1;
        }
        let mut f = std::fs::File::create(log_dir.join(format!("{version:020}.json")))?;
        f.write_all(content.as_bytes())?;
    }

    println!(
        "wrote {written} add actions across {commits} commits in {}",
        args.dir.display()
    );

    if args.checkpoint {
        use delta_kernel::engine::default::DefaultEngineBuilder;
        use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
        use delta_kernel::engine::default::storage::store_from_url_opts;

        // Snapshot::checkpoint requires the default engine to run on a
        // multi-threaded executor; the builder's default deadlocks here.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let executor = std::sync::Arc::new(TokioMultiThreadExecutor::new(rt.handle().clone()));

        let url = url::Url::from_directory_path(std::fs::canonicalize(&args.dir)?)
            .map_err(|_| "cannot build table URL")?;
        let store = store_from_url_opts(&url, std::collections::HashMap::<String, String>::new())?;
        let engine = DefaultEngineBuilder::new(store)
            .with_task_executor(executor)
            .build();
        let snapshot = delta_kernel::Snapshot::builder_for(url).build(&engine)?;
        let (result, _) = snapshot.checkpoint(&engine, None)?;
        println!("checkpoint written at the final version: {result:?}");
    }

    Ok(())
}
