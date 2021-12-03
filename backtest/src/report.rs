use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use plotly::common::Font;
use plotly::layout::{GridPattern, LayoutGrid, Legend, RowOrder};
use plotly::{Layout, Plot, Scatter};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use stats::Next;
use strategies::query::PortfolioSnapshot;
use strategies::types::StratEvent;
use strategies::StratEventLogger;
use trading::book::BookPosition;
use util::serde::write_as_seq;
use util::time::{now, now_str};

use crate::error::Result;

pub type TimedVec<T> = Vec<TimedData<T>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimedData<T> {
    ts: DateTime<Utc>,
    #[serde(flatten)]
    pub(crate) value: T,
}

impl<T> TimedData<T> {
    pub fn new(ts: DateTime<Utc>, value: T) -> Self { Self { ts, value } }
}

#[derive(Clone, Debug)]
pub struct VecEventLogger {
    events: Arc<Mutex<TimedVec<StratEvent>>>,
}

impl Default for VecEventLogger {
    fn default() -> Self {
        Self {
            events: Arc::new(Mutex::new(vec![])),
        }
    }
}
impl VecEventLogger {
    pub async fn get_events(&self) -> TimedVec<StratEvent> {
        let read = self.events.lock().await;
        read.clone()
    }
}

#[async_trait]
impl StratEventLogger for VecEventLogger {
    async fn maybe_log(&self, event: Option<StratEvent>) {
        if let Some(e) = event {
            let mut write = self.events.lock().await;
            write.push(TimedData::new(now(), e));
        }
    }
}

pub(crate) struct GlobalReport {
    reports: Vec<BacktestReport>,
    output_dir: PathBuf,
}

impl GlobalReport {
    pub(crate) fn new(output_dir: PathBuf) -> Self {
        Self {
            reports: vec![],
            output_dir,
        }
    }

    pub(crate) fn add_report(&mut self, report: BacktestReport) { self.reports.push(report); }

    pub(crate) fn write(&mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut report_dir = self.output_dir.clone();
        report_dir.push(now_str());
        for report in self.reports.as_slice() {
            report.write_to(report_dir.clone())?;
        }
        self.write_global_report(report_dir.as_path())?;
        self.symlink_dir(report_dir)?;
        Ok(())
    }

    pub fn write_global_report<P: AsRef<Path>>(
        &mut self,
        report_dir: P,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.write_pnl_report(&report_dir, "report.html", self.reports_by_pnl(10))?;
        self.write_pnl_report(&report_dir, "report_stddev.html", self.report_by_pnl_stddev(10))?;
        self.write_pnl_report(
            &report_dir,
            "report_increase_ratio.html",
            self.report_by_pnl_increase_ratio(10),
        )?;
        Ok(())
    }

    fn symlink_dir(&mut self, report_dir: PathBuf) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if cfg!(unix) {
            let mut ln_dir = self.output_dir.clone();
            ln_dir.push("latest");
            let path = std::env::current_dir()?.join(report_dir);
            if let Ok(true) = std::fs::try_exists(ln_dir.clone()) {
                std::fs::remove_file(ln_dir.clone()).unwrap();
            }
            std::os::unix::fs::symlink(path, ln_dir).unwrap();
        }
        Ok(())
    }

    pub fn write_pnl_report<'a, P: AsRef<Path>>(
        &self,
        output_dir: P,
        report_filename: &str,
        reports: impl Iterator<Item = (usize, &'a BacktestReport)>,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let out_file = format!("{}/{}", output_dir.as_ref().to_str().unwrap(), report_filename);
        let mut plot = Plot::new();
        for (i, report) in reports {
            draw_entries(&mut plot, i, report.indicators.as_slice(), vec![(
                &format!("{}.pnl", report.key),
                vec![|i| i.pnl],
            )]);
        }
        let layout = Layout::new()
            .grid(
                LayoutGrid::new()
                    .rows(10)
                    .columns(2)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::BottomToTop),
            )
            .legend(Legend::new().font(Font::new().size(10)));
        plot.set_layout(layout);
        plot.to_html(&out_file);
        Ok(out_file)
    }

    fn report_by_pnl_stddev(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(|r| !r.indicators.is_empty())
            .sorted_by_key(|r| (pnl_std_dev(r.indicators.as_slice()) * 1000.0) as u64)
            .take(top_n)
            .sorted_by_key(|r| r.indicators.last().unwrap().value.pnl as u64)
            .rev()
            .enumerate()
    }

    fn report_by_pnl_increase_ratio(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(|r| !r.indicators.is_empty())
            .sorted_by_key(|r| (pnl_increase_ratio(r.indicators.as_slice()) * 1000.0) as u64)
            .take(top_n)
            .sorted_by_key(|r| r.indicators.last().unwrap().value.pnl as u64)
            .rev()
            .enumerate()
    }

    fn reports_by_pnl(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(|r| !r.indicators.is_empty())
            .sorted_by_key(|r| r.indicators.last().unwrap().value.pnl as u64)
            .rev()
            .take(top_n)
            .rev()
            .enumerate()
    }
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct ReportSnapshot {
    model: Option<BTreeMap<String, Option<serde_json::Value>>>,
    indicators: Option<PortfolioSnapshot>,
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct BacktestReport {
    pub(crate) key: String,
    pub(crate) model_failures: u32,
    pub(crate) models: TimedVec<BTreeMap<String, Option<serde_json::Value>>>,
    pub(crate) indicator_failures: u32,
    pub(crate) indicators: TimedVec<PortfolioSnapshot>,
    pub(crate) book_positions: TimedVec<BookPosition>,
    pub(crate) events: TimedVec<StratEvent>,
}

impl BacktestReport {
    pub fn new(key: String) -> Self { Self { key, ..Self::default() } }

    pub fn write_to<P: AsRef<Path>>(&self, output_dir: P) -> Result<()> {
        let mut report_dir = output_dir.as_ref().to_path_buf();
        report_dir.push(self.key.clone());
        std::fs::create_dir_all(report_dir.clone())?;
        let mut file = report_dir.clone();
        file.push("models.json");
        write_as_seq(file, self.models.as_slice());
        let mut file = report_dir.clone();
        file.push("report.json");
        write_as_seq(file, vec![self].as_slice());
        let mut file = report_dir.clone();
        file.push("indicators.json");
        write_as_seq(file, self.indicators.as_slice());
        let mut file = report_dir.clone();
        file.push("strat_events.json");
        write_as_seq(file, self.events.as_slice());
        let mut file = report_dir.clone();
        file.push("final_values.json");
        let logs_f = std::fs::File::create(file).unwrap();
        let snapshot = ReportSnapshot {
            model: self.models.last().map(|m| m.value.clone()),
            indicators: self.indicators.last().map(|i| i.value.clone()),
        };
        serde_json::to_writer(logs_f, &snapshot).unwrap();
        self.write_html_report(report_dir).unwrap();
        Ok(())
    }

    fn write_html_report<P: AsRef<Path>>(
        &self,
        output_dir: P,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let out_file = format!("{}/report.html", output_dir.as_ref().to_str().unwrap());
        let mut plot = Plot::new();
        draw_entries(&mut plot, 0, self.models.as_slice(), vec![("apo", vec![|m| {
            m.get("apo").unwrap().as_ref().unwrap().as_f64().unwrap()
        }])]);
        draw_entries(&mut plot, 2, self.indicators.as_slice(), vec![
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
        let mut report = BacktestReport::new(key.to_string());
        report.indicators = read_json_file(path.as_ref(), "indicators.json");
        report
    }
}

pub type StrategyEntry<'a, T> = (&'a str, Vec<fn(&T) -> f64>);

fn draw_entries<T>(plot: &mut Plot, trace_offset: usize, data: &[TimedData<T>], entries: Vec<StrategyEntry<'_, T>>) {
    let skipped_data = data.iter();
    for (i, line_specs) in entries.iter().enumerate() {
        for (_, line_spec) in line_specs.1.iter().enumerate() {
            let time: Vec<DateTime<Utc>> = skipped_data.clone().map(|x| x.ts).collect();
            let y: Vec<f64> = skipped_data.clone().map(|td| line_spec(&td.value)).collect();
            let trace = Scatter::new(time, y)
                .name(line_specs.0)
                .x_axis(&format!("x{}", trace_offset + i))
                .y_axis(&format!("y{}", trace_offset + i));
            plot.add_trace(trace);
        }
    }
}

fn read_json_file<P: AsRef<Path>, T: DeserializeOwned>(base_path: P, filename: &str) -> T {
    let mut file = base_path.as_ref().to_path_buf();
    file.push(filename);
    let read = BufReader::new(File::open(file).unwrap());
    serde_json::from_reader(read).unwrap()
}

#[allow(dead_code)]
fn stop_event_ratio(events: &[StratEvent]) -> usize {
    events.len() / events.iter().filter(|e| matches!(e, StratEvent::Stop { .. })).count()
}

fn pnl_std_dev(indicators: &[TimedData<PortfolioSnapshot>]) -> f64 {
    let pnls: Vec<f64> = indicators.iter().map(|v| v.value.pnl).dedup().collect();
    let mut std_dev = stats::StandardDeviation::new(10).unwrap();
    pnls.iter().fold(0.0, |_, pnl| std_dev.next(*pnl))
}

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
