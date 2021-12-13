use std::collections::HashSet;
use std::path::PathBuf;

use chrono::{Date, Duration, TimeZone, Utc};
use futures::StreamExt;
use strategy::coinnect::broker::{Broker, UnboundedChannelMessageBroker};
use strategy::Channel;

use crate::error::*;
use crate::{csv_orderbooks_df, events_from_csv_orderbooks, events_from_orderbooks, raw_orderbooks_df,
            sampled_orderbooks_df};
use strategy::coinnect::prelude::{Exchange, Pair};
use strategy::coinnect::types::MarketEventEnvelope;
use util::time::DateRange;

pub struct Dataset {
    pub input_format: DatasetInputFormat,
    pub ds_type: DatasetType,
    pub base_dir: PathBuf,
    pub period: DateRange,
    pub input_sample_rate: Duration,
}

impl Dataset {
    pub async fn read_channels(
        &self,
        broker: UnboundedChannelMessageBroker<Channel, MarketEventEnvelope>,
    ) -> Result<()> {
        // TODO: get exchange from partition path once datafusion supports it
        let xch = broker.subjects().last().unwrap().exchange();
        for dt in self.period {
            let orderbook_partitions: HashSet<String> = broker
                .subjects()
                .filter(|c| matches!(c, Channel::Orderbooks { .. }))
                .map(|c| match c {
                    Channel::Orderbooks { xch, pair } => {
                        Some(self.ds_type.partition(self.base_dir.clone(), dt, *xch, pair))
                    }
                    _ => None,
                })
                .flatten()
                .collect();
            let event_stream = match self.ds_type {
                DatasetType::OrderbooksByMinute | DatasetType::OrderbooksBySecond => {
                    let input_format = self.input_format.to_string();
                    let record_batches = sampled_orderbooks_df(orderbook_partitions, input_format);
                    record_batches.map(|rb| events_from_orderbooks(xch, rb)).flatten()
                }
                // DatasetType::OrderbooksRaw => match self.input_format {
                //     DatasetInputFormat::Csv => {
                //         let records = csv_orderbooks_df(orderbook_partitions).await?;
                //         events_from_csv_orderbooks(*xch, pair.clone(), records.as_slice())
                //     }
                //     _ => {
                //         let records = raw_orderbooks_df(
                //             partitions,
                //             self.input_sample_rate,
                //             false,
                //             &self.input_format.to_string(),
                //         )
                //         .await?;
                //         events_from_orderbooks(*xch, records.as_slice())
                //     }
                // },
                _ => panic!("order books channel requires an order books dataset"),
            };
            event_stream.for_each(|event| async { broker.broadcast(event) }).await;
        }
        Ok(())
    }
}

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DatasetType {
    OrderbooksByMinute,
    OrderbooksBySecond,
    OrderbooksRaw,
    Trades,
}

impl DatasetType {
    pub(crate) fn partition(&self, base_dir: PathBuf, date: Date<Utc>, xch: Exchange, pair: &Pair) -> String {
        let ts = date.and_hms_milli(0, 0, 0, 0).timestamp_millis();
        let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d");
        let sub_partition = match self {
            DatasetType::OrderbooksByMinute => vec![
                format!("xch={}", xch),
                format!("chan={}", "1mn_order_books"),
                format!("dt={}", dt_par),
            ],
            DatasetType::OrderbooksBySecond => vec![
                format!("xch={}", xch),
                format!("chan={}", "1s_order_books"),
                format!("dt={}", dt_par),
            ],
            DatasetType::OrderbooksRaw => vec![
                xch.to_string(),
                "order_books".to_string(),
                format!("pr={}", pair),
                format!("dt={}", dt_par),
            ],
            DatasetType::Trades => vec![
                xch.to_string(),
                "trades".to_string(),
                format!("pr={}", pair),
                format!("dt={}", dt_par),
            ],
        };
        let mut partition_file = base_dir;
        for part in sub_partition {
            partition_file.push(part);
        }
        partition_file.as_path().to_string_lossy().to_string()
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
