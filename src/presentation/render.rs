mod json;
mod text;

use crate::error::Result;
use crate::presentation::Presentation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    pub fn render(self, presentation: &Presentation) -> Result<String> {
        match self {
            OutputFormat::Text => Ok(text::render(presentation)),

            OutputFormat::Json => json::render(presentation),
        }
    }
}

pub fn gate_failures(presentation: &Presentation) -> Vec<String> {
    text::gate_failures(presentation)
}
