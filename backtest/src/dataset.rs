use std::collections::HashSet;
use std::path::PathBuf;
use std::pin::Pin;

use chrono::{Date, Duration, TimeZone, Utc};
use futures::{pin_mut, Stream, StreamExt};

use strategy::coinnect::prelude::{Exchange, Pair};
use strategy::coinnect::types::MarketEventEnvelope;
use strategy::Channel;
use util::time::DateRange;

use crate::coinnect::broker::{AsyncBroker, ChannelMessageBroker};
use crate::error::*;
use crate::{flat_orderbooks_df, raw_orderbooks_df, sampled_orderbooks_df};

pub struct Dataset {
    pub input_format: DatasetInputFormat,
    pub ds_type: MarketEventDatasetType,
    pub base_dir: PathBuf,
    pub period: DateRange,
    pub input_sample_rate: Duration,
}

impl Dataset {
    pub async fn read_market_events(&self, broker: ChannelMessageBroker<Channel, MarketEventEnvelope>) -> Result<()> {
        for dt in self.period {
            let orderbook_partitions: HashSet<(PathBuf, Vec<(&'static str, String)>)> = broker
                .subjects()
                .filter(|c| matches!(c, Channel::Orderbooks { .. }))
                .filter_map(|c| match c {
                    Channel::Orderbooks { xch, pair } => {
                        Some(self.ds_type.partition(self.base_dir.clone(), dt, *xch, pair))
                    }
                    _ => None,
                })
                .collect();
            let input_format = self.input_format.to_string();
            let stream: Pin<Box<dyn Stream<Item = MarketEventEnvelope>>> = match self.ds_type {
                MarketEventDatasetType::OrderbooksByMinute | MarketEventDatasetType::OrderbooksBySecond => {
                    Box::pin(sampled_orderbooks_df(orderbook_partitions, input_format))
                }
                MarketEventDatasetType::OrderbooksRaw => Box::pin(raw_orderbooks_df(
                    orderbook_partitions,
                    self.input_sample_rate,
                    input_format,
                )),
                MarketEventDatasetType::OrderbooksFlat => {
                    Box::pin(flat_orderbooks_df(orderbook_partitions, input_format, 5))
                }
                MarketEventDatasetType::Trades => panic!("order books channel requires an order books dataset"),
            };
            pin_mut!(stream);
            stream.for_each(|event| broker.broadcast(event)).await;
            // for event in stream.next().await {
            //     broker.broadcast(event).await;
            // }
        }
        Ok(())
    }
}

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MarketEventDatasetType {
    /// Downsampled orderbooks, by minute
    OrderbooksByMinute,
    /// Downsampled orderbooks, by second
    OrderbooksBySecond,
    /// Raw orderbooks, by the millisecond
    OrderbooksRaw,
    /// Raw orderbooks, in a flat file
    OrderbooksFlat,
    /// Trades
    Trades,
}

impl MarketEventDatasetType {
    ///
    ///
    /// # Arguments
    ///
    /// * `base_dir`: a base directory
    /// * `date`: a date
    /// * `xch`: an exchange
    /// * `pair`: a market pair
    ///
    /// returns: (String, Vec<(String, String), Global>) the base directory and partitions
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    pub(crate) fn partition(
        self,
        base_dir: PathBuf,
        date: Date<Utc>,
        xch: Exchange,
        pair: &Pair,
    ) -> (PathBuf, Vec<(&'static str, String)>) {
        let ts = date.and_hms_milli(0, 0, 0, 0).timestamp_millis();
        let dt_par = Utc.timestamp_millis(ts).format("%Y%m%d").to_string();
        match self {
            MarketEventDatasetType::OrderbooksByMinute => (base_dir, vec![
                ("xch", xch.to_string()),
                ("chan", "1mn_order_books".to_string()),
                ("dt", dt_par),
            ]),
            MarketEventDatasetType::OrderbooksBySecond => (base_dir, vec![
                ("xch", xch.to_string()),
                ("chan", "1s_order_books".to_string()),
                ("dt", dt_par),
            ]),
            MarketEventDatasetType::OrderbooksRaw | MarketEventDatasetType::OrderbooksFlat => {
                (base_dir.join(xch.to_string()).join("order_books"), vec![
                    ("pr", pair.to_string()),
                    ("dt", dt_par),
                ])
            }
            MarketEventDatasetType::Trades => (base_dir.join(xch.to_string()).join("trades"), vec![
                ("xch", xch.to_string()),
                ("chan", "trades".to_string()),
                ("pr", pair.to_string()),
                ("dt", dt_par),
            ]),
        }
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
