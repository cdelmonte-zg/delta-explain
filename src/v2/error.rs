//! delta-explain's error type.
//!
//! Kernel errors pass through transparently (log replay, scanning, and cloud
//! storage all surface as [`delta_kernel::Error`]); the other variants name
//! the failure domains delta-explain owns itself. Before this enum existed,
//! everything was funneled through `delta_kernel::Error::Generic(String)`.

/// Errors produced by delta-explain.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Errors surfaced by delta-kernel-rs: log replay, metadata scans,
    /// snapshot resolution, object-store access made through the kernel.
    #[error(transparent)]
    Kernel(#[from] delta_kernel::Error),

    /// The predicate could not be classified or lowered: invalid SQL, an
    /// unsupported construct, or a literal that cannot be typed.
    #[error("{0}")]
    Predicate(String),

    /// The table path or URI could not be resolved.
    #[error("{0}")]
    TableUri(String),

    /// Invalid command-line input (e.g. a malformed --option value).
    #[error("{0}")]
    Options(String),

    /// Reading the Delta log directly through the object store failed
    /// (currently only the partition-columns reader takes this path).
    #[error("{0}")]
    Storage(String),

    /// AWS profile resolution failed (missing profile, no static keys, or a
    /// mechanism like SSO that delta-explain does not resolve).
    #[error("{0}")]
    Credentials(String),

    /// Rendering the report failed (JSON serialization).
    #[error("Cannot render output: {0}")]
    Render(#[from] serde_json::Error),

    /// The table uses a protocol feature this tool cannot analyze honestly.
    #[error("{0}")]
    UnsupportedTable(String),

    /// Writing the --debug-ir diagnostic dump failed (file creation, a
    /// section write, or installing the kernel trace subscriber).
    #[error("Cannot write debug IR dump: {0}")]
    DebugDump(String),

    /// Writing the report to stdout failed. A broken pipe never reaches
    /// this variant: the CLI layer swallows it and keeps the gate verdict.
    #[error("Cannot write output: {0}")]
    Output(std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
