use delta_kernel::Engine;

use crate::v2::analysis;
use crate::v2::analysis::model::AnalysisResult;
use crate::v2::error::Result;
use crate::v2::gates::{self, GateConfig, GateOutcome};
use crate::v2::report::{self, Report};
use crate::v2::table::TableState;

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub report: Report,
    pub gates: GateOutcome,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutionInput<'a> {
    pub table_path: &'a str,
    pub predicate: Option<&'a str>,
    pub gate_config: GateConfig,
}

pub fn execute(
    input: ExecutionInput<'_>,
    table: &TableState,
    engine: &dyn Engine,
) -> Result<ExecutionResult> {
    let analysis = analyze(input.predicate, table, engine)?;

    let gate_context = gates::context(table, analysis.as_ref());

    let gates = gates::evaluate(gate_context, input.gate_config);

    let report = report::build(input.table_path, input.predicate, table, analysis.as_ref());

    Ok(ExecutionResult { report, gates })
}

fn analyze(
    predicate: Option<&str>,
    table: &TableState,
    engine: &dyn Engine,
) -> Result<Option<AnalysisResult>> {
    predicate
        .map(|predicate| analysis::analyze(predicate, table, engine))
        .transpose()
}
