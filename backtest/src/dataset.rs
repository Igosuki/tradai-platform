use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use chrono::{Date, Duration, Utc};
use futures::{Stream, StreamExt};
use itertools::Itertools;

use crate::datasources::orderbook::{flat_orderbooks_df, raw_orderbooks_df, sampled_orderbooks_df};
use brokers::broker::{Broker, ChannelMessageBroker};
use brokers::prelude::{Exchange, Pair};
use brokers::types::{AssetType, MarketEventEnvelope};
use stats::kline::{Resolution, TimeUnit};
use strategy::Channel;
use util::time::DateRange;

use crate::datasources::trades::{candles_df, trades_df};
use crate::error::*;

/// The base directory of historical cache data, uses the env var COINDATA_CACHE_DIR
#[must_use]
pub fn data_cache_dir() -> PathBuf {
    Path::new(&std::env::var("COINDATA_CACHE_DIR").unwrap_or("".to_string())).join("data")
}

pub struct DatasetCatalog {
    datasets: HashMap<String, DatasetReader>,
}

impl DatasetCatalog {
    pub fn get_reader(&self, channel: &Channel) -> DatasetReader {
        match channel {
            Channel::Orders { .. } => todo!(),
            Channel::Trades { .. } | Channel::Candles { .. } => self.datasets.get("trades").unwrap(),
            Channel::Orderbooks { .. } => self.datasets.get("orderbooks").unwrap(),
        }
        .clone()
    }
}

pub fn default_data_catalog() -> DatasetCatalog {
    let mut datasets = HashMap::new();
    datasets.insert("orderbooks".to_string(), DatasetReader {
        input_format: DataFormat::Avro,
        ds_type: MarketEventDatasetType::OrderbooksByMinute,
        base_dir: data_cache_dir(),
        input_sample_rate: Duration::seconds(1),
        candle_resolution_period: TimeUnit::Second,
        candle_resolution_unit: 0,
    });
    datasets.insert("trades".to_string(), DatasetReader {
        input_format: DataFormat::Parquet,
        ds_type: MarketEventDatasetType::Trades,
        base_dir: data_cache_dir(),
        input_sample_rate: Duration::seconds(1),
        candle_resolution_period: TimeUnit::Minute,
        candle_resolution_unit: 15,
    });
    DatasetCatalog { datasets }
}

#[cfg(test)]
pub fn default_test_data_catalog() -> DatasetCatalog {
    let mut datasets = HashMap::new();
    datasets.insert("orderbooks".to_string(), DatasetReader {
        input_format: DataFormat::Avro,
        ds_type: MarketEventDatasetType::OrderbooksByMinute,
        base_dir: util::test::test_data_dir(),
        input_sample_rate: Duration::seconds(1),
        candle_resolution_period: TimeUnit::Second,
        candle_resolution_unit: 0,
    });
    datasets.insert("trades".to_string(), DatasetReader {
        input_format: DataFormat::Parquet,
        ds_type: MarketEventDatasetType::Trades,
        base_dir: util::test::test_data_dir(),
        input_sample_rate: Duration::seconds(1),
        candle_resolution_period: TimeUnit::MilliSecond,
        candle_resolution_unit: 200,
    });
    DatasetCatalog { datasets }
}

#[derive(Clone)]
pub struct DatasetReader {
    pub input_format: DataFormat,
    pub ds_type: MarketEventDatasetType,
    pub base_dir: PathBuf,
    pub input_sample_rate: Duration,
    pub candle_resolution_period: TimeUnit,
    pub candle_resolution_unit: u32,
}

impl DatasetReader {
    pub async fn read_channel_to_stream<'a, I>(
        &self,
        channels: I,
        dt: Date<Utc>,
    ) -> Pin<Box<dyn Stream<Item = MarketEventEnvelope>>>
    where
        I: Iterator<Item = &'a Channel>,
    {
        let (ob_chan_iter, rest) = channels.tee();
        let (trade_chan_iter, candle_chan_iter) = rest.tee();
        let orderbook_partitions: HashSet<(PathBuf, Vec<(&'static str, String)>)> = ob_chan_iter
            .filter(|c| matches!(c, Channel::Orderbooks { .. }))
            .filter_map(|c| match c {
                Channel::Orderbooks { xch, pair } => {
                    Some(self.ds_type.partition(self.base_dir.clone(), dt, *xch, pair, None))
                }
                _ => None,
            })
            .collect();
        let trades_partitions: HashSet<(PathBuf, Vec<(&'static str, String)>)> = trade_chan_iter
            .filter(|c| matches!(c, Channel::Trades { .. }))
            .filter_map(|c| match c {
                Channel::Trades { xch, pair } => {
                    Some(self.ds_type.partition(self.base_dir.clone(), dt, *xch, pair, None))
                }
                _ => None,
            })
            .collect();
        let candles_partitions: HashSet<(PathBuf, Vec<(&'static str, String)>)> = candle_chan_iter
            .filter(|c| matches!(c, Channel::Candles { .. }))
            .filter_map(|c| match c {
                Channel::Candles { xch, pair } => {
                    Some(self.ds_type.partition(self.base_dir.clone(), dt, *xch, pair, None))
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
            MarketEventDatasetType::Trades => Box::pin(futures::stream::select(
                trades_df(trades_partitions, input_format.clone()),
                candles_df(
                    candles_partitions,
                    input_format.clone(),
                    Some(Resolution::new(
                        self.candle_resolution_period,
                        self.candle_resolution_unit,
                    )),
                ),
            )),
        };
        stream
    }

    pub async fn stream_broker(
        &self,
        broker: &ChannelMessageBroker<Channel, MarketEventEnvelope>,
        period: DateRange,
    ) -> Result<()> {
        for dt in period {
            let stream = self.read_channel_to_stream(Broker::subjects(broker), dt).await;
            stream.for_each(|event| Broker::broadcast(broker, event)).await;
        }
        Ok(())
    }

    pub async fn read_all_events(&self, channels: &[Channel], period: DateRange) -> Result<Vec<MarketEventEnvelope>> {
        let mut events = vec![];
        for dt in period {
            let stream = self.read_channel_to_stream(channels.into_iter(), dt).await;
            events.extend(stream.collect::<Vec<MarketEventEnvelope>>().await);
        }
        Ok(events)
    }
}

#[allow(dead_code)]
pub struct Dataset {
    name: String,
    base_dir: PathBuf,
    partition_cols: Vec<String>,
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
        asset_type: Option<AssetType>,
    ) -> (PathBuf, Vec<(&'static str, String)>) {
        let dt_par = date.format("%Y%m%d").to_string();
        match self {
            MarketEventDatasetType::OrderbooksByMinute => (base_dir, vec![
                ("chan", "1mn_order_books".to_string()),
                ("xch", xch.to_string()),
                ("dt", dt_par),
            ]),
            MarketEventDatasetType::OrderbooksBySecond => (base_dir, vec![
                ("chan", "1s_order_books".to_string()),
                ("xch", xch.to_string()),
                ("dt", dt_par),
            ]),
            MarketEventDatasetType::OrderbooksRaw => (base_dir, vec![
                ("chan", "raw_order_books".to_string()),
                ("xch", xch.to_string()),
                ("pr", pair.to_string()),
                ("dt", dt_par),
            ]),
            MarketEventDatasetType::OrderbooksFlat => (base_dir, vec![
                ("chan", "flat_order_books".to_string()),
                ("xch", xch.to_string()),
                ("pr", pair.to_string()),
                ("dt", dt_par),
            ]),
            MarketEventDatasetType::Trades => (base_dir, vec![
                ("chan", "trades".to_string()),
                ("xch", xch.to_string()),
                ("asset", asset_type.unwrap_or(AssetType::Spot).as_ref().to_string()),
                ("pr", pair.to_string().replace('_', "")),
                ("dt", dt_par),
            ]),
        }
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DataFormat {
    Avro,
    Parquet,
    Csv,
}

impl ToString for DataFormat {
    fn to_string(&self) -> String {
        match self {
            DataFormat::Avro => "AVRO",
            DataFormat::Parquet => "PARQUET",
            DataFormat::Csv => "CSV",
        }
        .to_string()
    }
}
