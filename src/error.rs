#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Errors surfaced by delta-kernel-rs: log replay, metadata scans,
    /// snapshot resolution, object-store access made through the kernel.
    #[error(transparent)]
    Kernel(#[from] delta_kernel::KernelError),

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

    /// The HTTP exchange with the catalog failed before a response could be
    /// read: connection, TLS, timeout. Carries the full cause chain, since
    /// reqwest's top-level message alone ("error sending request") says
    /// nothing about why.
    #[error("Catalog request failed: {0}")]
    CatalogRequest(String),

    /// The catalog answered with an error (its body is kept verbatim), or
    /// with a response this tool cannot interpret.
    #[error("Catalog error {0}")]
    Catalog(String),
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        let mut message = err.to_string();
        let mut source = std::error::Error::source(&err);
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Error::CatalogRequest(message)
    }
}

impl Error {
    pub fn from_filesystem_snapshot(err: delta_kernel::KernelError) -> Self {
        match err {
            delta_kernel::KernelError::MaxCatalogVersion(_) => Self::UnsupportedTable(
                "table is catalog-managed: delta-explain does not support \
                     catalog-managed tables yet"
                    .to_owned(),
            ),
            other => Self::Kernel(other),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
