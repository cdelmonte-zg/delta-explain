mod diagnostics;
mod json;
mod text;

use crate::v2::error::Result;
use crate::v2::gates::GateOutcome;
use crate::v2::report::Report;

pub fn text(report: &Report, explain_why: bool) -> String {
    text::render(report, explain_why)
}

pub fn json(
    report: &Report,
    gates: &GateOutcome,
    elapsed_ms: u128,
    explain_why: bool,
) -> Result<String> {
    json::render(report, gates, elapsed_ms, explain_why)
}

pub fn gate_failures(outcome: &GateOutcome) -> Vec<String> {
    text::gate_failures(outcome)
}
