use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use plotly::{Plot, Scatter};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use strategy::types::StratEvent;
use strategy::StratEventLogger;
use util::time::now;

mod global;
mod single;

pub(crate) use global::GlobalReport;
pub(crate) use single::BacktestReport;

pub type TimedVec<T> = Vec<TimedData<T>>;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
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
