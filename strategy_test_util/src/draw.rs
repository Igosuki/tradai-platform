use std::error::Error;

use chrono::{DateTime, Utc};
use multimap::MultiMap;
use plotly::layout::{GridPattern, Layout, LayoutGrid};
use plotly::{Plot, Scatter};

use util::time::now_str_files;

pub type StrategyEntry<'a, T> = (&'a str, fn(&T) -> Vec<(&'a str, f64)>);

pub trait TimedEntry {
    fn time(&self) -> DateTime<Utc>;
}

pub fn draw_line_plot<T: TimedEntry>(
    module_path: &str,
    data: Vec<T>,
    entries: &[StrategyEntry<'_, T>],
) -> std::result::Result<String, Box<dyn Error>> {
    let graph_dir = format!("{}/graphs", util::test::test_results_dir(module_path),);
    std::fs::create_dir_all(&graph_dir).unwrap();
    let out_file = format!("{}/plot_{}.html", graph_dir, now_str_files());
    let mut plot = Plot::new();
    let time: Vec<DateTime<Utc>> = data.iter().map(|x| x.time()).collect();
    let entry_count = entries.len();
    for (i, line_specs) in entries.iter().enumerate() {
        let mut lines = MultiMap::new();
        for v in data.iter().map(line_specs.1).flatten() {
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
    plot.to_html(&out_file);
    Ok(out_file)
}
