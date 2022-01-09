use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use chrono::{DateTime, Utc};
use plotly::{Plot, Scatter};
use serde::de::DeserializeOwned;

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
    serde_json::from_reader(read).expect(filename)
}
