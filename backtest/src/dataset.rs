use std::collections::HashSet;
use std::path::PathBuf;

use chrono::{TimeZone, Utc};

use strategies::{Exchange, Pair};
use util::time::DateRange;

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Dataset {
    OrderbooksByMinute,
    OrderbooksBySecond,
    OrderbooksRaw,
    Trades,
}

impl Dataset {
    pub(crate) fn partitions(
        &self,
        data_dir: PathBuf,
        period: DateRange,
        xch: Exchange,
        pair: &Pair,
    ) -> HashSet<String> {
        let mut partitions = HashSet::new();
        for date in period {
            let ts = date.and_hms_milli(0, 0, 0, 0).timestamp_millis();
            let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d");
            let sub_partition = match self {
                Dataset::OrderbooksByMinute => vec![
                    format!("xch={}", xch),
                    format!("chan={}", "1mn_order_books"),
                    format!("dt={}", dt_par),
                ],
                Dataset::OrderbooksBySecond => vec![
                    format!("xch={}", xch),
                    format!("chan={}", "1s_order_books"),
                    format!("dt={}", dt_par),
                ],
                Dataset::OrderbooksRaw => vec![
                    xch.to_string(),
                    "order_books".to_string(),
                    format!("pr={}", pair),
                    format!("dt={}", dt_par),
                ],
                Dataset::Trades => vec![
                    xch.to_string(),
                    "trades".to_string(),
                    format!("pr={}", pair),
                    format!("dt={}", dt_par),
                ],
            };
            let mut partition_file = data_dir.clone();
            for part in sub_partition {
                partition_file.push(part);
            }
            partitions.insert(partition_file.as_path().to_string_lossy().to_string());
        }
        partitions
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DatasetInputFormat {
    Avro,
    Parquet,
    Csv,
}

impl ToString for DatasetInputFormat {
    fn to_string(&self) -> String {
        match self {
            DatasetInputFormat::Avro => "AVRO",
            DatasetInputFormat::Parquet => "PARQUET",
            DatasetInputFormat::Csv => "CSV",
        }
        .to_string()
    }
}
