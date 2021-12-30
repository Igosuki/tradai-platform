use crate::test_db_with_path;
use coinnect_rt::exchange::Exchange;
use std::path::Path;
use std::sync::Arc;
use strategy::plugin::StrategyPluginContext;
use trading::engine::mock_engine;

pub fn test_plugin_context<S: AsRef<Path>>(db_path: S, exchanges: &[Exchange]) -> StrategyPluginContext {
    StrategyPluginContext::builder()
        .db(test_db_with_path(db_path.as_ref()))
        .engine(Arc::new(mock_engine(db_path, exchanges)))
        .logger(None)
        .build()
}
