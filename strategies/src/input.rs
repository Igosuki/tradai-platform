use crate::model::BookPosition;
use chrono::prelude::*;
use chrono::{DateTime, Utc};
use glob::glob;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Result;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use util::date::DateRange;
use util::serdes::date_time_format;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvRecord {
    #[serde(with = "date_time_format")]
    #[serde(alias = "hourofday")]
    pub event_ms: DateTime<Utc>,
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
    pairs: Vec<String>,
    glob_str: &str,
) -> Vec<Vec<CsvRecord>> {
    let get_records = move |p: String| {
        dr.clone()
            .flat_map(|dt| {
                let date = dt;
                let buf = base_path
                    .join(format!("pr={}", p.clone()))
                    .join(format!("dt={}", date.format("%Y-%m-%d")));
                trace!("Loading csv records from : {:?}", buf);
                let files = glob(&format!("{}{}", buf.to_str().unwrap(), glob_str)).unwrap();
                files.flat_map(|p| load_records(p.unwrap().to_str().unwrap()))
            })
            .collect()
    };
    pairs.iter().map(|p| get_records(p.to_string())).collect()
}

fn load_records(path: &str) -> Vec<CsvRecord> {
    read_csv(path).unwrap()
}

pub fn to_pos(r: &CsvRecord) -> BookPosition {
    let asks = [
        (r.a1, r.aq1),
        (r.a2, r.aq2),
        (r.a3, r.aq3),
        (r.a4, r.aq4),
        (r.a5, r.aq5),
    ];
    let bids = [
        (r.b1, r.bq1),
        (r.b2, r.bq2),
        (r.b3, r.bq3),
        (r.b4, r.bq4),
        (r.b5, r.bq5),
    ];
    BookPosition::new(&asks, &bids)
}

pub async fn dl_test_data(
    base_path: Arc<String>,
    exchange_name: Arc<String>,
    channel: Arc<String>,
    pair: String,
) {
    let out_file_name = format!("{}.zip", pair);
    let file = tempfile::tempdir().unwrap();
    let out_file = file.into_path().join(out_file_name);
    let s3_key = &format!("test_data/{}/{}/{}.zip", exchange_name, channel, pair);
    util::s3::download_file(&s3_key.clone(), out_file.clone())
        .await
        .unwrap();

    let bp = base_path.deref();

    Command::new("unzip")
        .arg(&out_file)
        .arg("-d")
        .arg(bp)
        .output()
        .expect("failed to unzip file");
}

pub async fn load_csv_dataset(
    dr: &DateRange,
    pairs: Vec<String>,
    exchange_name: &str,
    channel: &str,
) -> Vec<Vec<CsvRecord>> {
    let bp = std::env::var_os("BITCOINS_REPO")
        .and_then(|oss| oss.into_string().ok())
        .unwrap_or_else(|| "..".to_string());

    let base_path = Path::new(&bp)
        .join("data")
        .join(exchange_name)
        .join(channel.clone());
    let bpc = Arc::new(bp);
    let channelc = Arc::new(channel.to_string());
    let exchange_namec = Arc::new(exchange_name.to_string());

    for s in pairs.clone() {
        if !base_path.exists() || !base_path.join(&format!("pr={}", s)).exists() {
            println!("download dataset from spaces");
            std::fs::create_dir_all(&base_path).unwrap();
            crate::input::dl_test_data(bpc.clone(), exchange_namec.clone(), channelc.clone(), s)
                .await;
        }
    }
    crate::input::load_records_from_csv(dr, &base_path, pairs, "*csv")
}
