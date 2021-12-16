use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use plotly::layout::{GridPattern, LayoutGrid};
use plotly::{Layout, Plot};
use tokio::sync::mpsc::UnboundedSender;

use stats::Next;
use strategy::query::PortfolioSnapshot;
use strategy::types::StratEvent;
use trading::types::MarketStat;
use util::ser::{write_as_seq, StreamSerializerWriter};

use crate::error::Result;

use super::TimedData;

#[derive(Serialize, Deserialize)]
pub(crate) struct BacktestReportMiscStats {
    pub(crate) pnl_std_dev: stats::StandardDeviation,
    pub(crate) pnl_std_dev_last: f64,
    pub(crate) pnl_inc_ratio: f64,
    pub(crate) count: u64,
    pub(crate) loss_count: u64,
    pub(crate) last_pnl: Option<f64>,
}

impl BacktestReportMiscStats {
    fn update(&mut self, new_pnl: f64) {
        if let Some(last_pnl) = self.last_pnl {
            if last_pnl != new_pnl {
                self.pnl_std_dev_last = self.pnl_std_dev.next(new_pnl);
                self.count += 1;
                if new_pnl < last_pnl {
                    self.loss_count += 1;
                }
                self.pnl_inc_ratio = (self.count * self.count) as f64 / (self.loss_count + 1) as f64;
            }
        }
        self.last_pnl = Some(new_pnl);
    }
}

impl Default for BacktestReportMiscStats {
    fn default() -> Self {
        Self {
            pnl_std_dev: stats::StandardDeviation::new(10).unwrap(),
            pnl_std_dev_last: 0.0,
            pnl_inc_ratio: 0.0,
            count: 0,
            loss_count: 0,
            last_pnl: None,
        }
    }
}

type TimedModelValue = TimedData<BTreeMap<String, Option<serde_json::Value>>>;

#[derive(Serialize)]
pub(crate) struct BacktestReport {
    pub(crate) output_dir: PathBuf,
    pub(crate) key: String,
    pub(crate) failures: u32,
    #[serde(skip)]
    pub(crate) model_ss: Arc<StreamSerializerWriter<TimedModelValue>>,
    #[serde(skip)]
    pub(crate) snapshots_ss: Arc<StreamSerializerWriter<TimedData<PortfolioSnapshot>>>,
    #[serde(skip)]
    pub(crate) market_stats_ss: Arc<StreamSerializerWriter<TimedData<MarketStat>>>,
    #[serde(skip)]
    pub(crate) events_ss: Arc<StreamSerializerWriter<TimedData<StratEvent>>>,
    pub(crate) execution_hist: HashMap<String, f64>,
    pub(crate) last_ptf_snapshot: Option<TimedData<PortfolioSnapshot>>,
    pub(crate) misc_stats: BacktestReportMiscStats,
}

impl Debug for BacktestReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BacktestReport")
            .field("key", &self.key)
            .field("output_dir", &self.output_dir)
            .finish()
    }
}

const MODEL_FILE: &str = "models.json";
const SNAPSHOTS_FILE: &str = "snapshots.json";
const MARKET_STATS_FILE: &str = "market_stats.json";
const STRAT_EVENTS_FILE: &str = "strat_events.json";
const REPORT_FILE: &str = "report.json";
const REPORT_HTML_FILE: &str = "report.html";

impl BacktestReport {
    /// Create a new backtest report
    /// Call [`start`] to enable writing data to files
    /// [`start`]: fn@self::BacktestReport::start
    pub fn new<P: AsRef<Path>>(base_output_dir: P, key: String) -> Self {
        let report_dir = base_output_dir.as_ref().to_path_buf().join(key.clone());
        Self {
            output_dir: report_dir.clone(),
            model_ss: Arc::new(StreamSerializerWriter::new(report_dir.join(MODEL_FILE))),
            snapshots_ss: Arc::new(StreamSerializerWriter::new(report_dir.join(SNAPSHOTS_FILE))),
            market_stats_ss: Arc::new(StreamSerializerWriter::new(report_dir.join(MARKET_STATS_FILE))),
            events_ss: Arc::new(StreamSerializerWriter::new(report_dir.join(STRAT_EVENTS_FILE))),
            misc_stats: BacktestReportMiscStats::default(),
            key,
            failures: Default::default(),
            execution_hist: Default::default(),
            last_ptf_snapshot: None,
        }
    }

    /// Push a model to the report
    pub fn push_model(&self, v: TimedData<BTreeMap<String, Option<serde_json::Value>>>) { self.model_ss.push(v); }

    /// Push a portfolio snapshot to the report
    pub fn push_snapshot(&mut self, v: TimedData<PortfolioSnapshot>) {
        self.snapshots_ss.push(v);
        // Only compute stddev if values change
        self.misc_stats.update(v.value.pnl);
        self.last_ptf_snapshot = Some(v);
    }

    /// Push a market stat to the report
    pub fn push_market_stat(&self, v: TimedData<MarketStat>) { self.market_stats_ss.push(v); }

    /// Push a strat event to the report
    #[allow(dead_code)]
    pub fn push_strat_event(&self, v: TimedData<StratEvent>) { self.events_ss.push(v); }

    /// Get a strat event sink to forward to
    pub fn strat_event_sink(&self) -> UnboundedSender<TimedData<StratEvent>> { self.events_ss.sink() }

    /// Start writing received data to files in a streaming manner
    pub async fn start(&self) -> Result<()> {
        std::fs::create_dir_all(self.output_dir.clone())?;
        let x = self.model_ss.clone();
        tokio::spawn(async move { x.start().await });
        let x1 = self.snapshots_ss.clone();
        tokio::spawn(async move { x1.start().await });
        let x2 = self.market_stats_ss.clone();
        tokio::spawn(async move { x2.start().await });
        Ok(())
    }

    /// Finish writing the report
    pub async fn write_to<P: AsRef<Path>>(&self, output_dir: P) -> Result<()> {
        let report_dir = output_dir.as_ref().to_path_buf().join(self.key.clone());
        std::fs::create_dir_all(report_dir.clone())?;
        let file = report_dir.join(REPORT_FILE);
        write_as_seq(file, vec![self].as_slice());
        self.model_ss.close().await;
        self.snapshots_ss.close().await;
        self.market_stats_ss.close().await;
        self.events_ss.close().await;
        self.write_html_report(report_dir).unwrap();
        Ok(())
    }

    fn write_html_report<P: AsRef<Path>>(
        &self,
        output_dir: P,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let out_file = format!("{}/{}", output_dir.as_ref().to_str().unwrap(), REPORT_HTML_FILE);
        let mut plot = Plot::new();
        let models: Vec<TimedData<BTreeMap<String, Option<serde_json::Value>>>> =
            super::read_json_file(output_dir.as_ref(), MODEL_FILE);
        super::draw_entries(&mut plot, 0, models.as_slice(), vec![("apo", vec![|m| {
            m.get("apo").unwrap().as_ref().unwrap().as_f64().unwrap()
        }])]);
        drop(models);
        let indicators: Vec<TimedData<PortfolioSnapshot>> = super::read_json_file(output_dir.as_ref(), SNAPSHOTS_FILE);
        super::draw_entries(&mut plot, 2, indicators.as_slice(), vec![
            ("pnl", vec![|i| i.pnl]),
            ("value", vec![|i| i.value]),
            ("return", vec![|i| i.current_return]),
        ]);

        let layout = Layout::new().grid(LayoutGrid::new().rows(4).columns(1).pattern(GridPattern::Independent));
        plot.set_layout(layout);
        plot.to_html(&out_file);
        Ok(out_file)
    }

    pub fn from_files<P: AsRef<Path>>(key: &str, path: P) -> Self {
        let report = BacktestReport::new(path.as_ref().to_path_buf(), key.to_string());
        report
    }
}

#[allow(dead_code)]
fn pnl_increase_ratio(indicators: &[TimedData<PortfolioSnapshot>]) -> f64 {
    let pnls: Vec<f64> = indicators.iter().map(|v| v.value.pnl).dedup().collect();
    let count_losses = pnls
        .iter()
        .tuple_windows()
        .map(|(a, b)| a > b)
        .filter(|increased| !*increased)
        .count();
    (pnls.len() * pnls.len() / (count_losses + 1)) as f64
}

#[allow(dead_code)]
fn stop_event_ratio(events: &[StratEvent]) -> usize {
    events.len() / events.iter().filter(|e| matches!(e, StratEvent::Stop { .. })).count()
}
