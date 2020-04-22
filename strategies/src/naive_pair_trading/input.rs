use chrono::prelude::*;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Result;
use std::path::PathBuf;
use util::date::DateRange;
use util::serdes::date_time_format;

#[derive(Debug, Serialize, Deserialize)]
pub struct CsvRecord {
    #[serde(with = "date_time_format")]
    pub hourofday: DateTime<Utc>,
    pub a1: f64,
    pub aq1: f64,
    pub a2: f64,
    pub aq2: f64,
    pub a3: f64,
    pub aq3: f64,
    pub a4: f64,
    pub aq4: f64,
    pub a5: f64,
    pub aq5: f64,
    pub b1: f64,
    pub bq1: f64,
    pub b2: f64,
    pub bq2: f64,
    pub b3: f64,
    pub bq3: f64,
    pub b4: f64,
    pub bq4: f64,
    pub b5: f64,
    pub bq5: f64,
}

pub fn read_csv(path: &str) -> Result<Vec<CsvRecord>> {
    let f = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(f);
    let vec: Vec<CsvRecord> = rdr
        .deserialize()
        .map(|r| {
            let record: Result<CsvRecord> = r.map_err(|e| e.into());
            record.ok()
        })
        .while_some()
        .collect(); // just skip invalid rows
    Ok(vec)
}

pub fn partition_path(exchange: &str, ts: i64, channel: &str, pair: &str) -> Option<PathBuf> {
    let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d");
    Some(
        PathBuf::new()
            .join(exchange)
            .join(channel)
            .join(format!("pr={}", pair))
            .join(format!("dt={}", dt_par)),
    )
}

pub fn load_records_from_csv(
    dr: &DateRange,
    base_path: &PathBuf,
    left_pair: &str,
    right_pair: &str,
) -> (Vec<CsvRecord>, Vec<CsvRecord>) {
    let get_records = move |p: String| {
        dr.clone()
            .flat_map(|dt| {
                let date = dt.clone();
                load_records(
                    base_path
                        .join(format!("pr={}", p.clone()))
                        .join(format!("dt={}.csv", date.format("%Y-%m-%d")))
                        .to_str()
                        .unwrap(),
                )
            })
            .collect()
    };
    return (
        get_records(left_pair.to_string()),
        get_records(right_pair.to_string()),
    );
}

fn load_records(path: &str) -> Vec<CsvRecord> {
    let dt1 = read_csv(path).unwrap();
    dt1
}
