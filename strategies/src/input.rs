use std::fs::File;
use std::io::{BufReader, Result};
use std::iter::FromIterator;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use chrono::prelude::*;
use chrono::{DateTime, Utc};
use glob::glob;
use serde::{Deserialize, Serialize};

use util::date::{DateRange, DurationRangeType};
use util::serde::date_time_format;

use crate::types::BookPosition;

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

async fn dl_test_data(base_path: Arc<String>, exchange_name: Arc<String>, channel: Arc<String>, pair: String) {
    let out_file_name = format!("{}.zip", pair);
    let file = tempfile::tempdir().unwrap();
    let out_file = file.into_path().join(out_file_name);
    let s3_key = &format!("test_data/{}/{}/{}.zip", exchange_name, channel, pair);
    let output = util::s3::download_file(&s3_key.clone(), out_file.clone())
        .await
        .expect("s3 file downloaded");
    if let Some(1) = output.status.code() {
        println!(
            "s3 download failed : {}",
            std::str::from_utf8(output.stderr.as_slice()).unwrap()
        );
    }

    let bp = base_path.deref();

    Command::new("unzip")
        .arg(&out_file)
        .arg("-d")
        .arg(bp)
        .output()
        .expect("failed to unzip file");
}

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

impl From<CsvRecord> for BookPosition {
    fn from(csvr: CsvRecord) -> BookPosition {
        let asks = [
            (csvr.a1, csvr.aq1),
            (csvr.a2, csvr.aq2),
            (csvr.a3, csvr.aq3),
            (csvr.a4, csvr.aq4),
            (csvr.a5, csvr.aq5),
        ];
        let bids = [
            (csvr.b1, csvr.bq1),
            (csvr.b2, csvr.bq2),
            (csvr.b3, csvr.bq3),
            (csvr.b4, csvr.bq4),
            (csvr.b5, csvr.bq5),
        ];
        BookPosition::new(&asks, &bids)
    }
}

impl<'a> From<&'a CsvRecord> for BookPosition {
    fn from(csvr: &'a CsvRecord) -> Self { csvr.clone().into() }
}

pub fn read_csv(path: &str) -> Result<Vec<CsvRecord>> {
    let f = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(BufReader::new(f));
    let vec: Vec<CsvRecord> = rdr.deserialize().flatten().collect(); // just skip invalid rows
    Ok(vec)
}

pub fn load_records_from_csv<R>(dr: &DateRange, base_path: &Path, pairs: Vec<String>, glob_str: &str) -> Vec<Vec<R>>
where
    Vec<R>: FromIterator<CsvRecord>,
{
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

fn load_records(path: &str) -> Vec<CsvRecord> { read_csv(path).unwrap() }

// Loads the relevant csv dataset
// These csv datasets are downsampled feeds generated from avro data by spark the spark_flattener function (see the spark files in the parent project)
// If the files are missing from $BITCOINS_REPO/data, they will be downloaded from s3 / spaces
pub async fn load_csv_dataset(
    dr: &DateRange,
    pairs: Vec<String>,
    exchange_name: &str,
    channel: &str,
) -> Vec<Vec<CsvRecord>> {
    let bp = std::env::var_os("BITCOINS_REPO")
        .and_then(|oss| oss.into_string().ok())
        .unwrap_or_else(|| "..".to_string());

    let base_path = Path::new(&bp).join("data").join(exchange_name).join(channel);
    let bpc = Arc::new(bp);
    let channelc = Arc::new(channel.to_string());
    let exchange_namec = Arc::new(exchange_name.to_string());

    for s in pairs.clone() {
        if !base_path.exists() || !base_path.join(&format!("pr={}", s)).exists() {
            println!("downloading dataset from spaces");
            std::fs::create_dir_all(&base_path).unwrap();
            crate::input::dl_test_data(bpc.clone(), exchange_namec.clone(), channelc.clone(), s).await;
        }
    }
    crate::input::load_records_from_csv(dr, &base_path, pairs, "*csv")
}

pub async fn load_csv_records(
    from: Date<Utc>,
    to: Date<Utc>,
    pairs: Vec<&str>,
    exchange: &str,
    channel: &str,
) -> Vec<Vec<CsvRecord>> {
    let now = Instant::now();
    let csv_records = load_csv_dataset(
        &DateRange(from, to, DurationRangeType::Days, 1),
        pairs.into_iter().map(|s| s.to_string()).collect(),
        exchange,
        channel,
    )
    .await;
    let num_records = csv_records[0].len();
    assert!(num_records > 0, "no csv records could be read");
    info!(
        "Loaded {} csv records in {:.6} ms",
        num_records,
        now.elapsed().as_millis()
    );
    csv_records
}
