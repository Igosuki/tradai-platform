use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use plotly::layout::{GridPattern, LayoutGrid};
use plotly::{Layout, Plot, Scatter};

use strategies::query::StrategyIndicators;
use strategies::types::{BookPosition, StratEvent};
use util::serde::write_as_seq;
use util::time::now_str;

use crate::error::Result;

pub type TimedVec<T> = Vec<TimedData<T>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimedData<T> {
    ts: DateTime<Utc>,
    #[serde(flatten)]
    value: T,
}

impl<T> TimedData<T> {
    pub fn new(ts: DateTime<Utc>, value: T) -> Self { Self { ts, value } }
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
        self.write_html_report(&report_dir)?;
        Ok(())
    }

    fn write_html_report<P: AsRef<Path>>(
        &self,
        output_dir: P,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let out_file = format!("{}/report.html", output_dir.as_ref().to_str().unwrap());
        let mut plot = Plot::new();
        for (i, report) in self.reports.iter().enumerate() {
            draw_entries(&mut plot, i + 1, report.indicators.as_slice(), vec![("pnl", vec![
                |i| i.pnl,
            ])]);
        }
        let layout = Layout::new().grid(LayoutGrid::new().rows(4).columns(1).pattern(GridPattern::Independent));
        plot.set_layout(layout);
        plot.to_html(&out_file);
        Ok(out_file)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct BacktestReport {
    pub(crate) key: String,
    pub(crate) model_failures: u32,
    pub(crate) models: TimedVec<BTreeMap<String, Option<serde_json::Value>>>,
    pub(crate) indicator_failures: u32,
    pub(crate) indicators: TimedVec<StrategyIndicators>,
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
