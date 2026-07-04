//! Diagnostic dump of a run's intermediate representations (`--debug-ir`).
//!
//! The dump shows the predicate at every level it exists on during a run:
//! the SQL input, the owned AST before and after normalization, the
//! classification, the kernel `Predicate` lowered for each phase scan, and
//! the survivor counts the scans returned. delta-kernel's own `tracing`
//! events (at `delta_kernel=debug` by default; override the directive via
//! the `DELTA_EXPLAIN_DEBUG_FILTER` environment variable) are captured into
//! a buffer and appended as the final section.
//!
//! The format is explicitly unstable: it is a local diagnostic, not part of
//! the CLI/JSON contract, and may change between releases without notice.

use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};

/// Shared buffer the tracing subscriber writes into; drained as the final
/// dump section. A poisoned lock drops trace lines instead of panicking:
/// diagnostics must never take the run down.
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

pub struct DebugDump {
    out: File,
    kernel_trace: KernelTraceBuffer,
}

impl DebugDump {
    /// Creates the dump file, writes the header, and routes delta-kernel's
    /// tracing events into an in-memory buffer that [`DebugDump::finish`]
    /// appends as the last section. The global tracing subscriber can only
    /// be installed once per process, which holds for a CLI invocation.
    pub fn create(path: &str) -> Result<Self> {
        let mut out = File::create(path)
            .map_err(|e| Error::DebugDump(format!("cannot create '{path}': {e}")))?;
        writeln!(
            out,
            "delta-explain debug IR dump (tool_version: {})\n\
             format: unstable diagnostic output, not part of the CLI/JSON contract",
            env!("CARGO_PKG_VERSION")
        )
        .map_err(|e| Error::DebugDump(format!("cannot write header: {e}")))?;

        let kernel_trace = KernelTraceBuffer::default();
        let writer = kernel_trace.clone();
        let filter = tracing_subscriber::EnvFilter::try_from_env("DELTA_EXPLAIN_DEBUG_FILTER")
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("delta_kernel=debug"));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber).map_err(|e| {
            Error::DebugDump(format!("cannot install kernel trace subscriber: {e}"))
        })?;

        Ok(Self { out, kernel_trace })
    }

    pub fn section(&mut self, title: &str, body: &str) -> Result<()> {
        writeln!(self.out, "\n== {title} ==\n{}", body.trim_end())
            .map_err(|e| Error::DebugDump(format!("cannot write section '{title}': {e}")))
    }

    /// Appends the captured kernel trace as the final section and flushes.
    pub fn finish(mut self) -> Result<()> {
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
            .map_err(|e| Error::DebugDump(format!("cannot flush dump: {e}")))
    }
}
