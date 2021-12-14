use std::fs::File;
use std::io::{BufReader, Result};
use std::iter::FromIterator;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::time::Instant;

use chrono::prelude::*;
use chrono::{DateTime, Utc};
#[cfg(feature = "dialog_cli")]
use glob::glob;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::{MarketEvent, MarketEventEnvelope, Orderbook, Pair};
use util::ser::date_time_format;
use util::test::test_data_dir;
use util::time::{DateRange, DurationRangeType};

async fn dl_test_data(base_path: &str, exchange_name: &str, channel: &str, pair: String) {
    let out_file_name = format!("{}.zip", pair);
    let file = tempfile::tempdir().unwrap();
    let out_file = file.into_path().join(out_file_name);
    let s3_key = &format!("test_data/{}/{}/{}.zip", exchange_name, channel, pair);
    let output = util::s3::download_file(&s3_key.clone(), out_file.clone())
        .await
        .expect("s3 file downloaded");
    if let Some(1) = output.status.code() {
        error!(
            "s3 download failed : {}",
            std::str::from_utf8(output.stderr.as_slice()).unwrap()
        );
    } else {
        error!("{:?}", output);
        error!("{:?}", base_path);
    }

    Command::new("unzip")
        .arg(&out_file)
        .arg("-d")
        .arg(base_path)
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

impl CsvRecord {
    fn asks(&self) -> [(f64, f64); 5] {
        [
            (self.a1, self.aq1),
            (self.a2, self.aq2),
            (self.a3, self.aq3),
            (self.a4, self.aq4),
            (self.a5, self.aq5),
        ]
    }

    fn bids(&self) -> [(f64, f64); 5] {
        [
            (self.b1, self.bq1),
            (self.b2, self.bq2),
            (self.b3, self.bq3),
            (self.b4, self.bq4),
            (self.b5, self.bq5),
        ]
    }

    pub fn to_orderbook(&self, pair: Pair) -> Orderbook {
        Orderbook {
            timestamp: self.event_ms.timestamp_millis(),
            pair,
            asks: self.asks().into(),
            bids: self.bids().into(),
            last_order_id: None,
        }
    }
}

pub fn read_csv(path: &str) -> Result<Vec<CsvRecord>> {
    let f = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(BufReader::new(f));
    let vec: Vec<CsvRecord> = rdr.deserialize().flatten().collect(); // just skip invalid rows
    Ok(vec)
}

pub fn load_records_from_csv<R>(
    dr: &DateRange,
    base_path: &Path,
    pairs: Vec<String>,
    glob_str: &str,
) -> Vec<(String, Vec<R>)>
where
    Vec<R>: FromIterator<CsvRecord>,
{
    let get_records = move |pair: String| {
        dr.flat_map(|dt| {
            let date = dt;
            let buf = base_path
                .join(format!("pr={}", pair.clone()))
                .join(format!("dt={}", date.format("%Y-%m-%d")));
            let glob_string = format!("{}{}", buf.to_str().unwrap(), glob_str);
            trace!("Loading csv records from : {:?}", glob_string);
            let files = glob::glob(&glob_string).unwrap();
            if files.count() == 0 {
                trace!("no files found !");
            }
            let files = glob::glob(&glob_string).unwrap();
            files.flat_map(|glob_result| load_records(glob_result.unwrap().to_str().unwrap()))
        })
        .collect()
    };
    pairs
        .iter()
        .map(|pair| (pair.clone(), get_records(pair.to_string())))
        .collect()
}

fn load_records(path: &str) -> Vec<CsvRecord> { read_csv(path).unwrap() }

// Loads the relevant csv dataset
// These csv datasets are downsampled feeds generated from avro data by spark the spark_flattener function (see the spark files in the parent project)
// If the files are missing from $BITCOINS_REPO/data, they will be downloaded from s3 / spaces
pub async fn load_csv_dataset(
    dr: &DateRange,
    pairs: Vec<String>,
    exchange: &str,
    channel: &str,
) -> Vec<(String, Vec<CsvRecord>)> {
    let base_path = test_data_dir().join(exchange).join(channel);
    for pair_string in pairs.clone() {
        let pair_path = base_path.join(&format!("pr={}", pair_string));
        if !base_path.exists() || !pair_path.exists() {
            info!(
                "downloading dataset from spaces because {} does not exist",
                pair_path.to_str().unwrap_or("")
            );
            std::fs::create_dir_all(&base_path).unwrap();
            let dest_dir = test_data_dir().as_path().to_str().unwrap().to_string();
            dl_test_data(&dest_dir, exchange, channel, pair_string).await;
        }
    }
    load_records_from_csv(dr, &base_path, pairs, "*csv")
}

pub async fn load_csv_records(
    from: Date<Utc>,
    to: Date<Utc>,
    pairs: Vec<&str>,
    exchange: &str,
    channel: &str,
) -> Vec<(String, Vec<CsvRecord>)> {
    let now = Instant::now();
    let csv_records = load_csv_dataset(
        &DateRange(from, to, DurationRangeType::Days, 1),
        pairs.into_iter().map(|s| s.to_string()).collect(),
        exchange,
        channel,
    )
    .await;
    let num_records = csv_records[0].1.len();
    assert!(num_records > 0, "no csv records could be read");
    info!(
        "Loaded {} csv records in {:.6} ms",
        num_records,
        now.elapsed().as_millis()
    );
    csv_records
}

/// Loads events from csv data, sorted by time
pub async fn load_csv_events(
    from: Date<Utc>,
    to: Date<Utc>,
    pairs: Vec<&str>,
    exchange: &str,
    channel: &str,
) -> impl Iterator<Item = MarketEventEnvelope> {
    let records = load_csv_records(from, to, pairs, exchange, channel).await;
    let exchange = Exchange::from_str(exchange).unwrap();
    records
        .iter()
        .map(|(pair, csv_records)| {
            let pair: Pair = pair.as_str().into();
            csv_records.iter().map(move |csvr| {
                MarketEventEnvelope::new(
                    exchange,
                    pair.clone(),
                    MarketEvent::Orderbook(csvr.to_orderbook(pair.clone())),
                )
            })
        })
        .flatten()
        .sorted_by(|e1, e2| e1.e.time().cmp(&e2.e.time()))
}
