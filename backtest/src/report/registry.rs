use std::sync::{Arc, Mutex};

use multimap::MultiMap;
use once_cell::sync::OnceCell;

use crate::report::BacktestReport;

static DEFAULT_REPORT_REGISTRY: OnceCell<ReportFnRegistry> = OnceCell::new();

/// Default registry (global static).
pub fn default_report_registry() -> &'static ReportFnRegistry {
    DEFAULT_REPORT_REGISTRY.get_or_init(ReportFnRegistry::new)
}

/// A struct for registering report functions that will be executed after a backtest is ran for a strategy
pub struct ReportFnRegistry {
    report_fns: Mutex<MultiMap<String, Arc<dyn Fn(&mut BacktestReport) + Sync + Send>>>,
}

impl Default for ReportFnRegistry {
    fn default() -> Self {
        ReportFnRegistry {
            report_fns: Mutex::new(MultiMap::new()),
        }
    }
}

impl ReportFnRegistry {
    pub fn new() -> Self { Self::default() }

    pub fn register(&self, key: String, report_fn: Arc<dyn Fn(&mut BacktestReport) + Sync + Send>) {
        self.report_fns.lock().unwrap().insert(key, report_fn);
    }

    pub fn get_report_fns(&self, key: String) -> Option<Vec<Arc<dyn Fn(&mut BacktestReport) + Sync + Send>>> {
        self.report_fns.lock().unwrap().get_vec(&key).cloned()
    }
}

pub fn get_report_fns(key: String) -> Option<Vec<Arc<dyn Fn(&mut BacktestReport) + Sync + Send>>> {
    default_report_registry().get_report_fns(key)
}

/// Manually register a market pair with a market symbol
pub fn register_report_fn(key: String, report_fn: Arc<dyn Fn(&mut BacktestReport) + Sync + Send>) {
    default_report_registry().register(key, report_fn);
}

#[cfg(test)]
mod test {
    use crate::report::BacktestReport;
    use std::sync::Arc;
    use util::compress::Compression;

    use crate::report::registry::ReportFnRegistry;

    #[tokio::test]
    async fn registry_report_fns() {
        let registry = ReportFnRegistry::default();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register(
            "foo".to_string(),
            Arc::new(move |report: &mut BacktestReport| {
                tx.send(report.key.clone()).unwrap();
            }),
        );
        let report_fns = registry.get_report_fns("foo".to_string()).unwrap();
        let mut report = BacktestReport::new("", "report".to_string(), Compression::none());
        report_fns[0](&mut report);
        let report_key = rx.recv().await;
        assert_eq!(report_key, Some("report".to_string()));
    }
}
