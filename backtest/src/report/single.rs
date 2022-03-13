use arrow2::array::ArrayRef;
use float_cmp::approx_eq;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use brokers::types::Candle;
use ext::ResultExt;
use itertools::Itertools;
use plotly::layout::{GridPattern, LayoutGrid};
use plotly::{Layout, Plot};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task;

use stats::Next;
use strategy::query::PortfolioSnapshot;
use strategy::types::StratEvent;
use trading::types::MarketStat;
use util::compress::Compression;
use util::ser::{write_as_seq, NdJsonSerde, StreamSerializerWriter};

use crate::error::Result;

use super::TimedData;

#[derive(Serialize, Deserialize, Clone)]
pub struct BacktestReportMiscStats {
    pub pnl_std_dev: stats::ta_indicators::StandardDeviation,
    pub pnl_std_dev_last: f64,
    pub pnl_inc_ratio: f64,
    pub count: u64,
    pub loss_count: u64,
    pub last_pnl: Option<f64>,
}

impl BacktestReportMiscStats {
    fn update(&mut self, new_pnl: f64) {
        if let Some(last_pnl) = self.last_pnl {
            if !approx_eq!(f64, last_pnl, new_pnl) {
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
            pnl_std_dev: stats::ta_indicators::StandardDeviation::new(10).unwrap(),
            pnl_std_dev_last: 0.0,
            pnl_inc_ratio: 0.0,
            count: 0,
            loss_count: 0,
            last_pnl: None,
        }
    }
}

type TimedModelValue = TimedData<BTreeMap<String, Option<serde_json::Value>>>;

#[derive(Serialize, Clone)]
pub struct BacktestReport {
    pub(crate) output_dir: PathBuf,
    pub(crate) key: String,
    pub(crate) failures: u32,
    #[serde(skip)]
    pub(crate) model_ss: Arc<StreamSerializerWriter<TimedModelValue, NdJsonSerde>>,
    #[serde(skip)]
    pub(crate) snapshots_ss: Arc<StreamSerializerWriter<TimedData<PortfolioSnapshot>, NdJsonSerde>>,
    #[serde(skip)]
    pub(crate) market_stats_ss: Arc<StreamSerializerWriter<TimedData<MarketStat>, NdJsonSerde>>,
    #[serde(skip)]
    pub(crate) events_ss: Arc<StreamSerializerWriter<TimedData<StratEvent>, NdJsonSerde>>,
    #[serde(skip)]
    pub(crate) candles_ss: Arc<StreamSerializerWriter<TimedData<Candle>, NdJsonSerde>>,
    pub(crate) execution_hist: HashMap<String, f64>,
    pub(crate) last_ptf_snapshot: Option<TimedData<PortfolioSnapshot>>,
    pub(crate) misc_stats: BacktestReportMiscStats,
    pub(crate) compression: Compression,
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
const CANDLES_FILE: &str = "candles.json";
const REPORT_FILE: &str = "report.json";
const REPORT_HTML_FILE: &str = "report.html";
const TRADEVIEW_HTML_FILE: &str = "tradeview.html";

impl BacktestReport {
    /// Create a new backtest report
    /// Call [`start`] to enable writing data to files
    /// [`start`]: `fn@self::BacktestReport::start`
    pub fn new<P: AsRef<Path>>(base_output_dir: P, key: String, compression: Compression) -> Self {
        let report_dir = base_output_dir.as_ref().to_path_buf().join(key.clone());
        Self {
            output_dir: report_dir.clone(),
            model_ss: Arc::new(StreamSerializerWriter::new_with_compression(
                report_dir.join(MODEL_FILE),
                compression,
            )),
            snapshots_ss: Arc::new(StreamSerializerWriter::new_with_compression(
                report_dir.join(SNAPSHOTS_FILE),
                compression,
            )),
            market_stats_ss: Arc::new(StreamSerializerWriter::new_with_compression(
                report_dir.join(MARKET_STATS_FILE),
                compression,
            )),
            events_ss: Arc::new(StreamSerializerWriter::new_with_compression(
                report_dir.join(STRAT_EVENTS_FILE),
                compression,
            )),
            candles_ss: Arc::new(StreamSerializerWriter::new_with_compression(
                report_dir.join(CANDLES_FILE),
                compression,
            )),
            misc_stats: BacktestReportMiscStats::default(),
            key,
            failures: Default::default(),
            execution_hist: HashMap::default(),
            last_ptf_snapshot: None,
            compression,
        }
    }

    /// Push a model to the report
    pub(crate) fn push_model(&self, v: TimedData<BTreeMap<String, Option<serde_json::Value>>>) {
        self.model_ss.push(v).unwrap();
    }

    /// Push a portfolio snapshot to the report
    pub(crate) fn push_snapshot(&mut self, v: TimedData<PortfolioSnapshot>) {
        self.snapshots_ss.push(v).unwrap();
        // Only compute stddev if values change
        self.misc_stats.update(v.value.pnl);
        self.last_ptf_snapshot = Some(v);
    }

    /// Push a candle to the report
    pub(crate) fn push_candle(&self, v: TimedData<Candle>) { self.candles_ss.push(v).unwrap(); }

    /// Push a market stat to the report
    pub(crate) fn push_market_stat(&self, v: TimedData<MarketStat>) { self.market_stats_ss.push(v).unwrap(); }

    /// Push a strat event to the report
    #[allow(dead_code)]
    pub(crate) fn push_strat_event(&self, v: TimedData<StratEvent>) { self.events_ss.push(v).unwrap(); }

    /// Get a strat event sink to forward to
    pub(crate) fn strat_event_sink(&self) -> UnboundedSender<TimedData<StratEvent>> { self.events_ss.sink() }

    /// Read portfolio snapshot events
    pub fn snapshots(&self) -> Result<Vec<TimedData<PortfolioSnapshot>>> { self.snapshots_ss.read_all().err_into() }

    /// Read candle events
    pub fn candles(&self) -> Result<Vec<TimedData<Candle>>> { self.candles_ss.read_all().err_into() }

    /// Read strategy events
    pub fn strat_events(&self) -> Result<Vec<TimedData<StratEvent>>> { self.events_ss.read_all().err_into() }

    /// Read market events
    pub fn market_events(&self) -> Result<Vec<TimedData<MarketStat>>> { self.market_stats_ss.read_all().err_into() }

    /// Read models events
    pub fn models(&self) -> Result<Vec<TimedModelValue>> { self.model_ss.read_all().err_into() }

    /// Read miscellaneous stats
    pub fn misc_stats(&self) -> &BacktestReportMiscStats { &self.misc_stats }

    /// Get report events as a dataframe
    pub fn events_df(&self, table: &str) -> Result<Vec<ArrayRef>> {
        use arrow2::io::ndjson::read;
        use arrow2::io::ndjson::read::FallibleStreamingIterator;
        let batch_size = 2048;
        let get_reader = || match table {
            "snapshots" => self.snapshots_ss.reader(),
            "models" => self.model_ss.reader(),
            "candles" => self.candles_ss.reader(),
            "events" => self.events_ss.reader(),
            _ => unimplemented!(),
        };
        let mut reader = get_reader();
        match reader.has_data_left() {
            Ok(false) => return Ok(vec![]),
            Err(e) => return Err(e.into()),
            _ => {}
        }
        let data_type = read::infer(&mut reader, Some(10))?;

        let mut reader = read::FileReader::new(get_reader(), vec!["".to_string(); batch_size], None);

        let mut arrays = vec![];
        // `next` is IO-bounded
        while let Some(rows) = reader.next()? {
            // `deserialize` is CPU-bounded
            let array = read::deserialize(rows, data_type.clone())?;
            //let sa = array.as_any().downcast_ref::<StructArray>();
            arrays.push(array);
        }
        Ok(arrays)
    }

    /// Start writing received data to files in a streaming manner
    pub async fn start(&self) -> Result<()> {
        std::fs::create_dir_all(self.output_dir.clone())?;
        let x = self.model_ss.clone();
        tokio::spawn(async move { x.start().await });
        let x1 = self.snapshots_ss.clone();
        tokio::spawn(async move { x1.start().await });
        let x2 = self.market_stats_ss.clone();
        tokio::spawn(async move { x2.start().await });
        let x3 = self.events_ss.clone();
        tokio::spawn(async move { x3.start().await });
        let x4 = self.candles_ss.clone();
        tokio::spawn(async move { x4.start().await });
        Ok(())
    }

    /// Finish writing the report
    pub async fn finish(&self) -> Result<()> {
        let report_dir = self.output_dir.clone();
        write_as_seq(report_dir.join(REPORT_FILE), vec![self].as_slice())?;
        self.model_ss.close().await;
        self.snapshots_ss.close().await;
        self.market_stats_ss.close().await;
        self.events_ss.close().await;
        self.candles_ss.close().await;
        Ok(())
    }

    pub fn write_html(&mut self) {
        self.write_html_report();
        self.write_html_tradeview();
        self.write_custom_reports();
    }

    pub fn write_custom_reports(&mut self) {
        if let Some(reports) = super::registry::get_report_fns(self.key.clone()) {
            for report in reports {
                report(self)
            }
        }
    }

    pub fn write_plot(&self, plot: Plot, file_name: &str) -> String {
        let output_dir = self.output_dir.clone();
        let out_file = format!("{}/{}", output_dir.as_path().to_str().unwrap(), file_name);
        tracing::debug!("writing html to {}", out_file);
        plot.to_html(&out_file);
        out_file
    }

    fn write_html_report(&self) -> String { self.write_plot(self.report_plot(), REPORT_HTML_FILE) }

    fn write_html_tradeview(&self) -> String { self.write_plot(self.tradeview_plot(), TRADEVIEW_HTML_FILE) }

    pub async fn reload<P: AsRef<Path>>(key: &str, path: P, report_compression: Compression) -> Self {
        let report = BacktestReport::new(path.as_ref(), key.to_string(), report_compression);
        task::spawn_blocking(move || {
            report.write_html_report();
            report
        })
        .await
        .unwrap()
    }

    pub fn report_plot(&self) -> Plot {
        let mut plot = Plot::new();
        let mut trace_offset = 0;

        if let Ok(models) = self.model_ss.read_all() {
            super::draw_lines(&mut plot, trace_offset, models.as_slice(), vec![("model", vec![
                |m| extract_f64(m, "ppo"),
                |m| extract_f64(m, "high"),
                |m| extract_f64(m, "low"),
            ])]);
            trace_offset += 1;
        }
        if let Ok(market_stats) = self.market_stats_ss.read_all() {
            super::draw_lines(&mut plot, trace_offset, market_stats.as_slice(), vec![("stats", vec![
                |ms| ms.w_price,
            ])]);
            trace_offset += 1;
        }
        if let Ok(snapshots) = self.snapshots_ss.read_all() {
            super::draw_lines(&mut plot, trace_offset, snapshots.as_slice(), vec![
                ("pnl", vec![|i| i.pnl]),
                ("value", vec![|i| i.value]),
                ("return", vec![|i| i.current_return]),
            ]);
            trace_offset += 1;
        }

        let layout = Layout::new().grid(
            LayoutGrid::new()
                .rows(trace_offset + 1)
                .columns(1)
                .pattern(GridPattern::Independent),
        );
        plot.set_layout(layout);
        plot
    }

    pub fn tradeview_plot(&self) -> Plot {
        let mut plot = Plot::new();
        let trace_offset = 0;
        if let Ok(candles) = self.candles_ss.read_all() {
            super::draw_candles("price", &mut plot, trace_offset, candles.as_slice());
        }
        let layout = Layout::new().grid(
            LayoutGrid::new()
                .rows(trace_offset + 1)
                .columns(1)
                .pattern(GridPattern::Independent),
        );
        plot.set_layout(layout);
        plot
    }

    pub fn draw_tradeview(&self) -> String {
        let plot = self.tradeview_plot();
        plot.to_inline_html("tradeview")
    }

    pub fn json_tradeview(&self) -> String {
        let plot = self.tradeview_plot();
        plot.to_json()
    }

    pub fn draw_report(&self) -> String {
        let plot = self.report_plot();
        plot.to_inline_html("single_report")
    }

    pub fn json_report(&self) -> String {
        let plot = self.report_plot();
        plot.to_json()
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

fn extract_f64(m: &BTreeMap<String, Option<serde_json::Value>>, key: &str) -> f64 {
    m.get(key)
        .as_ref()
        .and_then(|t| t.as_ref().and_then(Value::as_f64))
        .unwrap_or(0.0)
}
