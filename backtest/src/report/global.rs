use std::path::{Path, PathBuf};

use itertools::Itertools;
use plotly::common::Font;
use plotly::layout::{GridPattern, LayoutGrid, Legend, RowOrder};
use plotly::{Layout, Plot};

use strategy::query::PortfolioSnapshot;
use util::time::now_str;

use super::single::BacktestReport;
use super::TimedData;

pub(crate) struct GlobalReport {
    pub reports: Vec<BacktestReport>,
    pub output_dir: PathBuf,
    pub base_dir: PathBuf,
}

impl GlobalReport {
    pub(crate) fn new(output_dir: PathBuf) -> Self {
        Self {
            reports: vec![],
            base_dir: output_dir.clone(),
            output_dir: output_dir.join(now_str()),
        }
    }

    pub(crate) fn add_report(&mut self, report: BacktestReport) { self.reports.push(report); }

    pub(crate) async fn write(&mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let output_dir = self.output_dir.clone();
        for report in self.reports.as_slice().iter() {
            report.finish().await?;
        }
        self.write_global_report(output_dir.clone())?;
        self.symlink_dir(output_dir.clone())?;
        Ok(())
    }

    pub(crate) fn len(&self) -> usize { self.reports.len() }

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

    pub fn write_pnl_report<'a, P: AsRef<Path>>(
        &self,
        output_dir: P,
        report_filename: &str,
        reports: impl Iterator<Item = (usize, &'a BacktestReport)>,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let out_file = format!("{}/{}", output_dir.as_ref().to_str().unwrap(), report_filename);
        let mut plot = Plot::new();
        for (i, report) in reports {
            let snapshots: Vec<TimedData<PortfolioSnapshot>> =
                super::read_json_file(report.output_dir.clone(), "snapshots.json");
            super::draw_entries(&mut plot, i, snapshots.as_slice(), vec![(
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
            .filter(has_pnl_change)
            .sorted_by_key(|r| (r.misc_stats.pnl_std_dev_last * 1000.0) as u64)
            .take(top_n)
            .sorted_by_key(last_pnl)
            .rev()
            .enumerate()
    }

    fn report_by_pnl_increase_ratio(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(has_pnl_change)
            .sorted_by_key(|m| (m.misc_stats.pnl_inc_ratio * 1000.0) as u64)
            .take(top_n)
            .sorted_by_key(last_pnl)
            .rev()
            .enumerate()
    }

    fn reports_by_pnl(&self, top_n: usize) -> impl Iterator<Item = (usize, &BacktestReport)> {
        self.reports
            .iter()
            .filter(has_pnl_change)
            .sorted_by_key(last_pnl)
            .rev()
            .take(top_n)
            .rev()
            .enumerate()
    }
}

fn has_pnl_change(r: &&BacktestReport) -> bool { r.misc_stats.count > 2 }

fn last_pnl(r: &&BacktestReport) -> u64 { r.misc_stats.last_pnl.unwrap_or(0.0) as u64 }
