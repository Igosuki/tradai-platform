use std::error::Error;

use chrono::{DateTime, Utc};
use plotly::layout::{GridPattern, Layout, LayoutGrid};
use plotly::{Plot, Scatter};

use util::time::now_str_files;

pub type StrategyEntry<'a, T> = (&'a str, Vec<fn(&T) -> f64>);

pub trait TimedEntry {
    fn time(&self) -> DateTime<Utc>;
}

pub fn draw_line_plot<T: TimedEntry>(
    module_path: &str,
    data: Vec<T>,
    entries: Vec<StrategyEntry<'_, T>>,
) -> std::result::Result<String, Box<dyn Error>> {
    let graph_dir = format!("{}/graphs", util::test::test_results_dir(module_path),);
    std::fs::create_dir_all(&graph_dir).unwrap();
    let out_file = format!("{}/plot_{}.html", graph_dir, now_str_files());
    let mut plot = Plot::new();
    let skipped_data = data.iter();
    for (i, line_specs) in entries.iter().enumerate() {
        for (_, line_spec) in line_specs.1.iter().enumerate() {
            let time: Vec<DateTime<Utc>> = skipped_data.clone().map(|x| x.time()).collect();
            let y: Vec<f64> = skipped_data.clone().map(line_spec).collect();
            let trace = Scatter::new(time, y)
                .name(line_specs.0)
                .x_axis(&format!("x{}", i + 1))
                .y_axis(&format!("y{}", i + 1));
            plot.add_trace(trace);
        }
    }
    let layout = Layout::new().grid(
        LayoutGrid::new()
            .rows(entries.len())
            .columns(1)
            .pattern(GridPattern::Independent),
    );
    plot.set_layout(layout);
    plot.to_html(&out_file);
    Ok(out_file)
}
