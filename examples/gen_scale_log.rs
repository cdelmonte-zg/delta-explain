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
//!
//! # a 10 TB profile: 40k files x 256 MB, 30 extra stats columns
//! # (wide production schemas), checkpointed
//! cargo run --release --example gen_scale_log -- /tmp/scale-10tb \
//!     --files 40000 --commits 400 --wide 30 --file-size-mb 256 --checkpoint
//! ```
//!
//! Layout: partitioned by `country` (200 values), an `age` int column whose
//! per-file ranges tile [i%100, i%100+10], so predicates have predictable
//! selectivity at any size; `--wide N` adds N more stats-bearing leaf
//! columns (strings with 28-char bounds, longs, doubles, timestamps, in
//! rotation), because a big production table's log weighs mostly in per-file
//! stats width, not file count. Timing procedure: /usr/bin/time -v on a
//! release binary, baseline and with a predicate; the numbers live in the
//! README's Performance notes.

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

    /// Extra stats-bearing data columns beyond `age` (wide production
    /// schemas; types rotate string/long/double/timestamp)
    #[arg(long, default_value_t = 0)]
    wide: usize,

    /// Declared size of each data file in MB (metadata only; the tool
    /// never reads data files)
    #[arg(long, default_value_t = 50)]
    file_size_mb: u64,
}

/// The per-file stats JSON: `age` tiles [lo, lo+10] for predictable
/// selectivity, and every `--wide` column carries realistic min/max
/// payloads (28-char string bounds, longs, doubles, ISO timestamps) so
/// the stats weigh what a wide production schema's do.
fn build_stats(i: usize, lo: usize, wide: usize) -> String {
    use std::fmt::Write as _;
    let mut min = String::new();
    let mut max = String::new();
    let mut nulls = String::new();
    write!(min, "{{\"age\":{lo}").ok();
    write!(max, "{{\"age\":{}", lo + 10).ok();
    write!(nulls, "{{\"age\":0").ok();
    for c in 0..wide {
        let name = format!("c{c:03}");
        match c % 4 {
            0 => {
                write!(min, ",\"{name}\":\"value-{i:016}-aaaaaaa\"").ok();
                write!(max, ",\"{name}\":\"value-{i:016}-zzzzzzz\"").ok();
            }
            1 => {
                write!(min, ",\"{name}\":{}", i as u64 * 1000).ok();
                write!(max, ",\"{name}\":{}", i as u64 * 1000 + 999).ok();
            }
            2 => {
                write!(min, ",\"{name}\":{}.25", i).ok();
                write!(max, ",\"{name}\":{}.75", i + 1).ok();
            }
            _ => {
                write!(
                    min,
                    ",\"{name}\":\"2026-01-01T00:{:02}:{:02}.000Z\"",
                    (i / 60) % 60,
                    i % 60
                )
                .ok();
                write!(
                    max,
                    ",\"{name}\":\"2026-01-01T12:{:02}:{:02}.000Z\"",
                    (i / 60) % 60,
                    i % 60
                )
                .ok();
            }
        }
        write!(nulls, ",\"{name}\":{}", i % 7).ok();
    }
    format!(
        "{{\"numRecords\":10000,\"minValues\":{min}}},\"maxValues\":{max}}},\"nullCount\":{nulls}}}}}"
    )
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

    let mut fields = vec![
        serde_json::json!({"name": "country", "type": "string", "nullable": true, "metadata": {}}),
        serde_json::json!({"name": "age", "type": "integer", "nullable": true, "metadata": {}}),
    ];
    const WIDE_TYPES: [&str; 4] = ["string", "long", "double", "timestamp"];
    for c in 0..args.wide {
        fields.push(serde_json::json!({
            "name": format!("c{c:03}"),
            "type": WIDE_TYPES[c % WIDE_TYPES.len()],
            "nullable": true,
            "metadata": {},
        }));
    }
    let schema = serde_json::json!({ "type": "struct", "fields": fields });
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
            let stats = build_stats(i, lo, args.wide);
            let add = serde_json::json!({"add": {
                "path": format!("country={country}/part-{i:07}.parquet"),
                "partitionValues": {"country": country},
                "size": args.file_size_mb * 1024 * 1024,
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
        use delta_kernel_default_engine::DefaultEngineBuilder;
        use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
        use delta_kernel_default_engine::storage::store_from_url_opts;

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

#[cfg(test)]
mod tests {
    use super::build_stats;

    // `build_stats` hand-builds the stats JSON with manual braces and
    // escapes; one misplaced brace yields invalid stats that delta-explain
    // silently treats as absent. Guard the shape directly. (Run with
    // `cargo test --examples`.)
    #[test]
    fn wide_stats_are_valid_json_with_a_leaf_per_column() {
        for wide in [0, 1, 4, 30] {
            let raw = build_stats(123, 45, wide);
            let v: serde_json::Value =
                serde_json::from_str(&raw).unwrap_or_else(|e| panic!("wide={wide}: {e}\n{raw}"));
            assert_eq!(v["numRecords"], 10000);
            // age plus one leaf per wide column, on every stats map.
            for key in ["minValues", "maxValues", "nullCount"] {
                let n = v[key].as_object().unwrap().len();
                assert_eq!(n, wide + 1, "wide={wide} {key} has {n} leaves");
            }
        }
    }

    #[test]
    fn all_four_wide_types_render_their_leaf() {
        // The rotation reaches every arm (string/long/double/timestamp) by
        // wide=4; each leaf must be present in min and max.
        let v: serde_json::Value = serde_json::from_str(&build_stats(7, 3, 4)).unwrap();
        for c in 0..4 {
            let name = format!("c{c:03}");
            assert!(v["minValues"].get(&name).is_some(), "missing min {name}");
            assert!(v["maxValues"].get(&name).is_some(), "missing max {name}");
        }
    }
}
