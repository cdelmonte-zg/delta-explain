use std::fmt::{Debug, Display};

use crate::error::Result;

pub trait Instrumentation {
    fn invocation(&mut self, _table: &str, _predicate: Option<&str>) -> Result<()> {
        Ok(())
    }

    fn snapshot_opened(
        &mut self,
        _version: u64,
        _files: usize,
        _partition_columns: &[String],
    ) -> Result<()> {
        Ok(())
    }

    fn predicate_parsed(&mut self, _rendered: &dyn Display, _debug: &dyn Debug) -> Result<()> {
        Ok(())
    }

    fn predicate_normalized(&mut self, _rendered: &dyn Display, _debug: &dyn Debug) -> Result<()> {
        Ok(())
    }

    fn classification_completed(&mut self, _classification: &dyn Debug) -> Result<()> {
        Ok(())
    }

    fn partition_kernel_predicate_lowered(
        &mut self,
        _source: &dyn Display,
        _lowered: &dyn Debug,
    ) -> Result<()> {
        Ok(())
    }

    fn scan_kernel_predicate_lowered(
        &mut self,
        _source: &dyn Display,
        _lowered: &dyn Debug,
    ) -> Result<()> {
        Ok(())
    }

    fn scan_without_predicate(&mut self) -> Result<()> {
        Ok(())
    }

    fn partition_evaluated(
        &mut self,
        _predicate: &dyn Display,
        _survivors: usize,
        _total: usize,
        _evaluation_gaps: usize,
    ) -> Result<()> {
        Ok(())
    }

    fn survivor_sets_computed(
        &mut self,
        _baseline: usize,
        _partition: Option<usize>,
        _scan: Option<usize>,
    ) -> Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NoOpInstrumentation;

impl Instrumentation for NoOpInstrumentation {}
