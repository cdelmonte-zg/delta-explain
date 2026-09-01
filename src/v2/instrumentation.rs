mod observer;

#[cfg(feature = "debug-ir")]
mod debug_ir;

pub use observer::{Instrumentation, NoOpInstrumentation};

#[cfg(feature = "debug-ir")]
pub use debug_ir::DebugIrObserver;
