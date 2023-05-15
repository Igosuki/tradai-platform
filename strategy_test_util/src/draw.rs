use std::error::Error;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use multimap::MultiMap;
use plotly::layout::{GridPattern, Layout, LayoutGrid};
use plotly::{Plot, Scatter};

use util::time::now_str_files;

pub type StrategyEntry<T, S> = (S, StrategyEntryFnRef<T, S>);
pub type StrategyEntryFnRef<T, S> = Arc<dyn Fn(&T) -> Vec<(S, f64)> + Send + Sync>;

pub trait TimedEntry {
    fn time(&self) -> DateTime<Utc>;
}

pub fn draw_line_plot<'a, T: 'a + TimedEntry + Clone, S2>(
    module_path: &str,
    data: &'a [T],
    entries: &[StrategyEntry<T, S2>],
) -> std::result::Result<String, Box<dyn Error>>
where
    S2: ToString + Sized,
{
    let graph_dir = format!("{}/graphs", module_path);
    std::fs::create_dir_all(&graph_dir)?;
    let out_file = format!("{}/plot_{}.html", graph_dir, now_str_files());
    let mut plot = Plot::new();
    let time: Vec<DateTime<Utc>> = data.iter().map(TimedEntry::time).collect();
    let entry_count = entries.len();
    for (i, (_entry_name, entry_fn)) in entries.iter().enumerate() {
        let mut lines: MultiMap<String, f64> = MultiMap::new();
        for v in data.iter().flat_map(|v| (entry_fn.as_ref())(v)) {
            lines.insert(v.0.to_string(), v.1);
        }
        for (_, (line_name, y)) in lines.into_iter().enumerate() {
            let trace = Scatter::new(time.clone(), y)
                .name(&line_name)
                .x_axis(&format!("x{}", i + 1))
                .y_axis(&format!("y{}", i + 1));
            plot.add_trace(trace);
        }
    }
    let layout = Layout::new().grid(
        LayoutGrid::new()
            .rows(entry_count)
            .columns(1)
            .pattern(GridPattern::Independent),
    );
    plot.set_layout(layout);
    plot.write_html(out_file.clone());
    Ok(out_file)
}
