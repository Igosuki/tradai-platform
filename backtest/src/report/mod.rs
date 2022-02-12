use brokers::types::Candle;
use chrono::{DateTime, TimeZone, Utc};
use plotly::{Ohlc, Plot, Scatter};

pub(crate) use global::GlobalReport;
pub(crate) use logger::StreamWriterLogger;
pub(crate) use single::BacktestReport;
use util::compress::Compression;
use util::time::TimedData;

mod global;
mod logger;
mod single;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReportConfig {
    pub parallelism: Option<usize>,
    pub compression: Compression,
}

pub type StrategyEntry<'a, T> = (&'a str, Vec<fn(&T) -> f64>);

#[allow(clippy::needless_pass_by_value)]
fn draw_lines<T>(plot: &mut Plot, trace_offset: usize, data: &[TimedData<T>], entries: Vec<StrategyEntry<'_, T>>) {
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

#[derive(Serialize)]
struct OHLCTime(DateTime<Utc>);

impl Default for OHLCTime {
    fn default() -> Self { OHLCTime(Utc.timestamp_millis(0)) }
}

#[allow(clippy::needless_pass_by_value)]
fn draw_ohlc(name: &str, plot: &mut Plot, _trace_offset: usize, data: &[TimedData<Candle>]) {
    let skipped_data = data.iter();
    let time: Vec<OHLCTime> = skipped_data.clone().map(|x| OHLCTime(x.ts)).collect();
    let open: Vec<f64> = skipped_data.clone().map(|td| td.value.open).collect();
    let high: Vec<f64> = skipped_data.clone().map(|td| td.value.high).collect();
    let low: Vec<f64> = skipped_data.clone().map(|td| td.value.low).collect();
    let close: Vec<f64> = skipped_data.clone().map(|td| td.value.close).collect();
    let trace = Ohlc::new(time, open, high, low, close).name(name);
    plot.add_trace(trace);
}
