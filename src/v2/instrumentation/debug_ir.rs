use std::fmt::{Debug, Display};
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::v2::error::{Error, Result};

use super::Instrumentation;

#[derive(Clone, Default)]
struct KernelTraceBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for KernelTraceBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(mut guard) = self.0.lock() {
            guard.extend_from_slice(buf);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct DebugIrObserver {
    out: File,

    kernel_trace: KernelTraceBuffer,

    finished: bool,
}

impl DebugIrObserver {
    pub fn create(path: &str) -> Result<Self> {
        let mut out = File::create(path)
            .map_err(|err| Error::DebugDump(format!("cannot create '{path}': {err}")))?;

        writeln!(
            out,
            "delta-explain debug IR dump (tool_version: {})\n\
             format: unstable diagnostic output, not part of the CLI/JSON contract",
            env!("CARGO_PKG_VERSION")
        )
        .map_err(|err| Error::DebugDump(format!("cannot write header: {err}")))?;

        let kernel_trace = KernelTraceBuffer::default();

        let writer = kernel_trace.clone();

        let filter = tracing_subscriber::EnvFilter::try_from_env("DELTA_EXPLAIN_DEBUG_FILTER")
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("delta_kernel=debug"));

        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();

        tracing::subscriber::set_global_default(subscriber).map_err(|err| {
            Error::DebugDump(format!("cannot install kernel trace subscriber: {err}"))
        })?;

        Ok(Self {
            out,
            kernel_trace,
            finished: false,
        })
    }

    fn section(&mut self, title: &str, body: &str) -> Result<()> {
        writeln!(self.out, "\n== {title} ==\n{}", body.trim_end())
            .map_err(|err| Error::DebugDump(format!("cannot write section '{title}': {err}")))
    }
}

impl Instrumentation for DebugIrObserver {
    fn invocation(&mut self, table: &str, predicate: Option<&str>) -> Result<()> {
        self.section(
            "invocation",
            &format!(
                "table: {table}\npredicate: {}",
                predicate.unwrap_or("(none)")
            ),
        )
    }

    fn snapshot_opened(
        &mut self,
        version: u64,
        files: usize,
        partition_columns: &[String],
    ) -> Result<()> {
        self.section(
            "snapshot",
            &format!(
                "version: {version}\n\
                 files in snapshot: {files}\n\
                 partition columns: {partition_columns:?}"
            ),
        )
    }

    fn predicate_parsed(&mut self, rendered: &dyn Display, debug: &dyn Debug) -> Result<()> {
        self.section(
            "owned AST (before normalization)",
            &format!("rendered: {rendered}\n\n{debug:#?}"),
        )
    }

    fn predicate_normalized(&mut self, rendered: &dyn Display, debug: &dyn Debug) -> Result<()> {
        self.section(
            "owned AST (normalized)",
            &format!("rendered: {rendered}\n\n{debug:#?}"),
        )
    }

    fn classification_completed(&mut self, classification: &dyn Debug) -> Result<()> {
        self.section("classification", &format!("{classification:#?}"))
    }

    fn partition_kernel_predicate_lowered(
        &mut self,
        source: &dyn Display,
        lowered: &dyn Debug,
    ) -> Result<()> {
        self.section(
            "kernel predicate: partition-only scan",
            &format!("lowered from: {source}\n\n{lowered:#?}"),
        )
    }

    fn scan_kernel_predicate_lowered(
        &mut self,
        source: &dyn Display,
        lowered: &dyn Debug,
    ) -> Result<()> {
        self.section(
            "kernel predicate: full scan",
            &format!(
                "scan predicate after stripping unsupported fragments: \
                 {source}\n\n{lowered:#?}"
            ),
        )
    }

    fn scan_without_predicate(&mut self) -> Result<()> {
        self.section(
            "kernel predicate: full scan",
            "no fragment survives the strip; \
             the full scan runs without a predicate",
        )
    }

    fn partition_evaluated(
        &mut self,
        predicate: &dyn Display,
        survivors: usize,
        total: usize,
        evaluation_gaps: usize,
    ) -> Result<()> {
        self.section(
            "partition-literal evaluation",
            &format!(
                "fragment: {predicate}\n\
                 survivors: {survivors} of {total} files \
                 ({evaluation_gaps} kept on evaluation gaps)"
            ),
        )
    }

    fn survivor_sets_computed(
        &mut self,
        baseline: usize,
        partition: Option<usize>,
        scan: Option<usize>,
    ) -> Result<()> {
        let partition_line =
            match partition {
                Some(files) => {
                    format!(
                        "partition pruning: {files} files"
                    )
                }

                None => {
                    "partition pruning: skipped \
                    (no partition fragment)"
                        .to_string()
                }
            };

        let scan_line = match scan {
            Some(files) => {
                format!("full scan: {files} files")
            }

            None => "full scan: skipped \
                     (pure-partition predicate)"
                .to_string(),
        };

        self.section(
            "survivor sets",
            &format!(
                "baseline: {baseline} files\n\
                 {partition_line}\n\
                 {scan_line}"
            ),
        )
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        let trace = match self.kernel_trace.0.lock() {
            Ok(guard) => String::from_utf8_lossy(&guard).into_owned(),

            Err(_) => String::new(),
        };

        let body = if trace.is_empty() {
            "(no events captured at the current filter)".to_string()
        } else {
            trace
        };

        self.section("kernel trace", &body)?;

        self.out
            .flush()
            .map_err(|err| Error::DebugDump(format!("cannot flush dump: {err}")))?;

        self.finished = true;

        Ok(())
    }
}
