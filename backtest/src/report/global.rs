use std::path::{Path, PathBuf};

use futures::StreamExt;
use itertools::Itertools;
use plotly::common::Font;
use plotly::layout::{GridPattern, LayoutGrid, Legend, RowOrder};
use plotly::{Layout, Plot};

use util::compress::Compression;
use util::time::now_str;

use super::single::BacktestReport;

pub struct GlobalReport {
    pub reports: Vec<BacktestReport>,
    pub output_dir: PathBuf,
    pub base_dir: PathBuf,
    parallelism: usize,
}

impl GlobalReport {
    pub(crate) fn new(output_dir: PathBuf) -> Self { Self::new_with(output_dir, None, Compression::default()) }

    pub(crate) fn new_with(output_dir: PathBuf, parallelism: Option<usize>, _compression: Compression) -> Self {
        let output_dir_path = output_dir.join(now_str());
        Self {
            reports: vec![],
            base_dir: output_dir,
            output_dir: output_dir_path,
            parallelism: parallelism.unwrap_or_else(num_cpus::get),
        }
    }

    pub(crate) fn add_report(&mut self, report: BacktestReport) { self.reports.push(report); }

    pub(crate) async fn write(&mut self) -> std::result::Result<(), Box<dyn std::error::Error + '_>> {
        let output_dir = self.output_dir.clone();
        tokio_stream::iter(self.reports.as_slice().iter())
            .map(|report| async move {
                report.finish().await?;
                tokio::task::block_in_place(move || report.write_html());
                crate::error::Result::Ok(())
            })
            .buffer_unordered(self.parallelism)
            .map(|x| {
                x.unwrap();
                Ok(())
            })
            .forward(futures::sink::drain())
            .await?;
        self.write_global_report(output_dir.clone());
        self.symlink_dir(output_dir)?;
        Ok(())
    }

    pub(crate) fn len(&self) -> usize { self.reports.len() }

    pub(crate) fn write_global_report<P: AsRef<Path>>(&mut self, report_dir: P) {
        self.write_pnl_report(&report_dir, "report.html", self.reports_by_pnl(10));
        self.write_pnl_report(&report_dir, "report_stddev.html", self.report_by_pnl_stddev(10));
        self.write_pnl_report(
            &report_dir,
            "report_increase_ratio.html",
            self.report_by_pnl_increase_ratio(10),
        );
    }

    fn symlink_dir(&mut self, report_dir: PathBuf) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if cfg!(unix) {
            let mut ln_dir = self.base_dir.clone();
            ln_dir.push("latest");
            let path = std::env::current_dir()?.join(report_dir);
            if let Ok(true) = std::fs::try_exists(ln_dir.clone()) {
                std::fs::remove_file(ln_dir.clone()).unwrap();
            }
            std::os::unix::fs::symlink(path, ln_dir).unwrap();
        }
        Ok(())
    }

    pub(crate) fn write_pnl_report<'a, P: AsRef<Path>>(
        &self,
        output_dir: P,
        report_filename: &str,
        reports: impl Iterator<Item = (usize, &'a BacktestReport)>,
    ) -> String {
        let out_file = format!("{}/{}", output_dir.as_ref().to_str().unwrap(), report_filename);
        let mut plot = Plot::new();
        for (i, report) in reports {
            if let Ok(snapshots) = report.snapshots() {
                super::draw_lines(&mut plot, i, snapshots.as_slice(), vec![(
                    &format!("{}.pnl", report.key),
                    vec![|i| i.pnl],
                )]);
            }
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
        out_file
    }

    fn report_by_pnl_stddev(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(|br| has_pnl_change(*br))
            .sorted_by_key(|r| (r.misc_stats.pnl_std_dev_last * 1000.0) as u64)
            .take(top_n)
            .sorted_by_key(|br| last_pnl(*br))
            .rev()
            .enumerate()
    }

    fn report_by_pnl_increase_ratio(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(|br| has_pnl_change(*br))
            .sorted_by_key(|m| (m.misc_stats.pnl_inc_ratio * 1000.0) as u64)
            .take(top_n)
            .sorted_by_key(|br| last_pnl(*br))
            .rev()
            .enumerate()
    }

    fn reports_by_pnl(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(|br| has_pnl_change(*br))
            .sorted_by_key(|br| last_pnl(*br))
            .rev()
            .take(top_n)
            .rev()
            .enumerate()
    }
}

fn has_pnl_change(r: &BacktestReport) -> bool { r.misc_stats.count > 2 }

fn last_pnl(r: &BacktestReport) -> u64 { r.misc_stats.last_pnl.unwrap_or(0.0) as u64 }
