#[derive(Debug, Clone)]
pub struct Presentation {
    pub elapsed_ms: u128,

    pub table: TableView,

    pub predicate: Option<String>,

    pub analysis: Option<AnalysisView>,

    pub phases: Vec<PhaseView>,

    pub final_files: usize,

    pub total_pruning_pct: f64,

    pub warnings: Vec<WarningView>,

    /// `None` means --explain-why was not requested.
    /// `Some([])` means it was requested but no diagnoses exist.
    pub explanations: Option<Vec<ExplanationView>>,

    pub assertions: Vec<AssertionView>,

    pub result: Option<StatusView>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PresentationOptions {
    pub explain_why: bool,
}

#[derive(Debug, Clone)]
pub struct TableView {
    pub path: String,
    pub version: u64,
    pub total_files: usize,

    pub stats: StatsView,

    pub features: TableFeaturesView,
}

#[derive(Debug, Clone)]
pub struct StatsView {
    pub mode: &'static str,
    pub files_with_stats: usize,
    pub total_files: usize,
    pub pct: f64,
}

#[derive(Debug, Clone)]
pub struct TableFeaturesView {
    pub deletion_vectors_enabled: bool,
    pub files_with_deletion_vectors: usize,

    pub column_mapping_mode: Option<String>,

    pub clustering_columns: Option<Vec<String>>,

    pub in_commit_timestamps: bool,

    pub unrecognized_writer_features: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AnalysisView {
    pub partition_safe: Option<String>,
    pub partition_exact: Option<String>,
    pub stats_safe: Option<String>,
    pub unsplittable: Option<String>,
    pub confidence: &'static str,
}

#[derive(Debug, Clone)]
pub struct PhaseView {
    pub name: &'static str,
    pub confidence: &'static str,

    /// Logical predicate attributed to the phase.
    /// This is the JSON contract value.
    pub predicate: String,

    /// What actually reached the pruning backend when unsupported
    /// fragments were stripped. Used only by text presentation.
    pub scanned_predicate: Option<String>,

    pub conservative_fragments: usize,

    pub input_files: usize,
    pub output_files: usize,
    pub pruned_files: usize,
    pub pruning_pct: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticScope {
    Analysis,
    Table,
}

#[derive(Debug, Clone)]
pub struct WarningView {
    /// Human-readable/text code.
    pub code: &'static str,

    /// Stable JSON 0.4.0 code.
    pub schema_code: &'static str,

    pub message: String,

    pub scope: DiagnosticScope,
}

#[derive(Debug, Clone)]
pub struct ExplanationView {
    pub code: &'static str,
    pub severity: &'static str,
    pub message: String,
    pub suggestion: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusView {
    Pass,
    Fail,
}

impl StatusView {
    pub fn as_str(self) -> &'static str {
        match self {
            StatusView::Pass => "pass",
            StatusView::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone)]
pub enum AssertionView {
    MinPruning {
        threshold: f64,
        actual: f64,
        status: StatusView,
    },

    StatsComplete {
        missing_files: Vec<String>,
        status: StatusView,
    },
}

impl AssertionView {
    pub fn status(&self) -> StatusView {
        match self {
            AssertionView::MinPruning { status, .. }
            | AssertionView::StatsComplete { status, .. } => *status,
        }
    }
}
