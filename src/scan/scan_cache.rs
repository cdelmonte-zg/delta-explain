use delta_kernel::{
    EngineData, FilteredEngineData, KernelResult, Version,
    arrow::array::RecordBatch,
    engine::arrow_data::{ArrowEngineData, EngineDataArrowExt},
};

pub(crate) struct CachedScanMetadata {
    version: Version,
    batches: Vec<RecordBatch>,
}

impl CachedScanMetadata {
    pub(crate) fn new(version: Version) -> Self {
        Self {
            version,
            batches: Vec::new(),
        }
    }

    pub(crate) fn insert(&mut self, data: FilteredEngineData) -> KernelResult<()> {
        self.batches
            .push(data.apply_selection_vector()?.try_into_record_batch()?);

        Ok(())
    }

    pub(crate) fn version(&self) -> Version {
        self.version
    }

    // just keeping the arrow buffers, but transferring the ownership of their referrers
    pub(crate) fn engine_data(&self) -> impl Iterator<Item = Box<dyn EngineData>> + Send + 'static {
        self.batches
            .clone()
            .into_iter()
            .map(|batch| -> Box<dyn EngineData> { Box::new(ArrowEngineData::new(batch)) })
    }
}
