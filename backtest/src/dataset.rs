use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use chrono::{Date, DateTime, Duration, Timelike, Utc};
use datafusion::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};

use crate::datasources::orderbook::{flat_orderbooks_stream, raw_orderbooks_df, raw_orderbooks_stream,
                                    sampled_orderbooks_df, sampled_orderbooks_stream};
use brokers::broker::{AsyncBroker, ChannelMessageBroker};
use brokers::prelude::{Exchange, Pair};
use brokers::types::{MarketEventEnvelope, SecurityType};
use strategy::{MarketChannel, MarketChannelTopic, MarketChannelType};
use util::time::DateRange;

use crate::datasources::trades::{candles_df, candles_stream, trades_df, trades_stream};
use crate::error::*;

// TODO: There should be some other way to load table definitions, maybe a json file or a data catalog format

pub fn coindata_data_cache_dir() -> PathBuf {
    Path::new(&std::env::var("COINDATA_CACHE_DIR").unwrap_or_else(|_| "".to_string())).to_path_buf()
}

#[derive(Clone)]
pub struct TableDef {
    #[allow(dead_code)]
    name: &'static str,
    format: DataFormat,
    base_dir: PathBuf,
}

#[derive(Clone)]
pub struct DatasetCatalog {
    pub catalog: HashMap<MarketEventDatasetType, TableDef>,
}

impl DatasetCatalog {
    pub fn get(&self, t: MarketEventDatasetType) -> Option<&TableDef> { self.catalog.get(&t) }

    pub fn default_basedir(base_dir: PathBuf) -> Self {
        Self::default_formats(base_dir.join("data"), base_dir.join("data24"))
    }

    pub fn default_prod() -> Self {
        Self::default_formats(
            coindata_data_cache_dir().join("data"),
            coindata_data_cache_dir().join("data24"),
        )
    }

    #[cfg(test)]
    pub fn default_test() -> Self { Self::default_formats(coindata_data_cache_dir(), util::test::test_data_dir()) }

    pub fn default_formats(base_data_dir: PathBuf, base_data24_dir: PathBuf) -> DatasetCatalog {
        let mut datasets = HashMap::new();
        datasets.insert(MarketEventDatasetType::OrderbooksByMinute, TableDef {
            name: "1mn_order_books",
            format: DataFormat::Avro,
            base_dir: base_data_dir.clone(),
        });
        datasets.insert(MarketEventDatasetType::OrderbooksBySecond, TableDef {
            name: "1s_order_books",
            format: DataFormat::Avro,
            base_dir: base_data_dir.clone(),
        });
        datasets.insert(MarketEventDatasetType::OrderbooksRaw, TableDef {
            name: "order_books",
            format: DataFormat::Avro,
            base_dir: base_data24_dir,
        });
        datasets.insert(MarketEventDatasetType::Trades, TableDef {
            name: "trades",
            format: DataFormat::Parquet,
            base_dir: base_data_dir,
        });
        DatasetCatalog { catalog: datasets }
    }
}

pub type PartitionSet = HashSet<(PathBuf, Vec<(&'static str, String)>)>;

#[derive(Debug)]
pub struct Dataset {
    pub channel: MarketChannel,
    pub r#type: MarketEventDatasetType,
    pub partitions: PartitionSet,
    pub format: DataFormat,
}

#[derive(Clone)]
pub struct DatasetReader {
    pub catalog: DatasetCatalog,
}

impl DatasetReader {
    fn datasets<'a, I>(&self, channels: I, dt: Date<Utc>) -> Vec<Dataset>
    where
        I: Iterator<Item = &'a MarketChannel>,
    {
        let mut datasets = vec![];
        for channel in channels {
            let ds_type = match channel.r#type {
                MarketChannelType::Orderbooks => match channel.tick_rate {
                    Some(tr) => {
                        if tr <= Duration::seconds(1) {
                            MarketEventDatasetType::OrderbooksBySecond
                        } else if tr <= Duration::minutes(1) {
                            MarketEventDatasetType::OrderbooksByMinute
                        } else {
                            MarketEventDatasetType::OrderbooksRaw
                        }
                    }
                    None => MarketEventDatasetType::OrderbooksRaw,
                },
                MarketChannelType::Trades | MarketChannelType::Candles => MarketEventDatasetType::Trades,
                _ => unimplemented!(),
            };
            let table_def = self.catalog.get(ds_type).unwrap();
            let mut partitions = HashSet::new();
            partitions.insert(ds_type.partition(
                table_def.base_dir.clone(),
                dt,
                channel.symbol.xch,
                &channel.symbol.value,
                Some(channel.symbol.r#type),
            ));
            datasets.push(Dataset {
                channel: channel.clone(),
                r#type: ds_type,
                partitions,
                format: table_def.format.clone(),
            });
        }
        datasets
    }

    pub async fn read_channels_to_stream<'a, I>(
        &self,
        channels: I,
        lower_dt: DateTime<Utc>,
        upper_dt: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Stream<Item = MarketEventEnvelope>>>
    where
        I: Iterator<Item = &'a MarketChannel>,
    {
        let datasets = self.datasets(channels, lower_dt.date());
        let lower_dt = (lower_dt.num_seconds_from_midnight() != 0).then(|| lower_dt);
        let stream: Pin<Box<dyn Stream<Item = MarketEventEnvelope>>> =
            Box::pin(futures::stream::select_all(datasets.iter().map(|ds| {
                let input_format = ds.format.to_string();
                let partitions = ds.partitions.clone();
                let inner: Pin<Box<dyn Stream<Item = MarketEventEnvelope>>> = match ds.r#type {
                    MarketEventDatasetType::OrderbooksByMinute | MarketEventDatasetType::OrderbooksBySecond => {
                        Box::pin(sampled_orderbooks_stream(partitions, input_format, lower_dt, upper_dt))
                    }
                    MarketEventDatasetType::OrderbooksRaw => Box::pin(raw_orderbooks_stream(
                        partitions,
                        ds.channel.tick_rate.unwrap_or_else(|| Duration::minutes(1)),
                        input_format,
                    )),
                    MarketEventDatasetType::OrderbooksFlat => {
                        Box::pin(flat_orderbooks_stream(partitions, input_format, 5))
                    }
                    MarketEventDatasetType::Trades => match ds.channel.r#type {
                        MarketChannelType::Trades => {
                            Box::pin(trades_stream(partitions, input_format, lower_dt, upper_dt))
                        }
                        MarketChannelType::Candles => Box::pin(candles_stream(
                            partitions,
                            input_format,
                            lower_dt,
                            upper_dt,
                            ds.channel.resolution,
                        )),
                        _ => unimplemented!(),
                    },
                };
                inner
            })));
        stream
    }

    pub async fn read_channels_to_df<'a, I>(
        &self,
        channels: I,
        lower_dt: DateTime<Utc>,
        upper_dt: Option<DateTime<Utc>>,
    ) -> Result<Vec<RecordBatch>>
    where
        I: Iterator<Item = &'a MarketChannel>,
    {
        let datasets = self.datasets(channels, lower_dt.date());
        let lower_dt = (lower_dt.num_seconds_from_midnight() != 0).then(|| lower_dt);
        let rb = futures::future::try_join_all(datasets.iter().map(|ds| {
            let input_format = ds.r#type.default_format().to_string();
            let partitions = ds.partitions.clone();
            let fut: BoxFuture<Result<RecordBatch>> = match ds.r#type {
                MarketEventDatasetType::OrderbooksByMinute | MarketEventDatasetType::OrderbooksBySecond => {
                    Box::pin(sampled_orderbooks_df(partitions, input_format, lower_dt, upper_dt))
                }

                MarketEventDatasetType::OrderbooksRaw => Box::pin(raw_orderbooks_df(
                    partitions,
                    ds.channel.tick_rate.unwrap_or_else(|| Duration::minutes(1)),
                    input_format,
                    lower_dt,
                    upper_dt,
                )),
                //MarketEventDatasetType::OrderbooksFlat => Box::pin(flat_orderbooks_stream(partitions, input_format, 5)),
                MarketEventDatasetType::Trades => match ds.channel.r#type {
                    MarketChannelType::Trades => Box::pin(trades_df(partitions, input_format, lower_dt, upper_dt)),
                    MarketChannelType::Candles => Box::pin(candles_df(
                        partitions,
                        input_format,
                        lower_dt,
                        upper_dt,
                        ds.channel.resolution,
                    )),
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            };
            fut
        }))
        .await?;
        Ok(rb)
    }

    pub async fn stream_with_broker(
        &self,
        channels: &[MarketChannel],
        broker: &ChannelMessageBroker<MarketChannelTopic, MarketEventEnvelope>,
        period: DateRange,
    ) -> Result<()> {
        for dt in period {
            let stream = self
                .read_channels_to_stream(channels.iter(), dt, period.upper_bound_in_range())
                .await;
            stream.for_each(|event| AsyncBroker::broadcast(broker, event)).await;
        }
        Ok(())
    }

    pub async fn read_all_events(
        &self,
        channels: &[MarketChannel],
        period: DateRange,
    ) -> Result<Vec<MarketEventEnvelope>> {
        let mut events = vec![];
        for dt in period {
            let stream = self
                .read_channels_to_stream(channels.iter(), dt, period.upper_bound_in_range())
                .await;
            events.extend(stream.collect::<Vec<MarketEventEnvelope>>().await);
        }
        Ok(events)
    }

    pub async fn read_all_events_df(&self, channels: &[MarketChannel], period: DateRange) -> Result<Vec<RecordBatch>> {
        let mut events = vec![];
        for dt in period {
            let rbs = self
                .read_channels_to_df(channels.iter(), dt, period.upper_bound_in_range())
                .await?;
            events.extend(rbs);
        }
        Ok(events)
    }
}

#[derive(Debug, Deserialize, Copy, Clone, Hash, PartialEq, Eq)]
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
        sec_type: Option<SecurityType>,
    ) -> (PathBuf, Vec<(&'static str, String)>) {
        let dt_par = date.format("%Y%m%d").to_string();
        let asset_str = sec_type
            .map(|st| match st {
                SecurityType::Equity => "eqty",
                SecurityType::Option => "opt",
                SecurityType::Commodity => "comm",
                SecurityType::Forex => "frx",
                SecurityType::Future => "futures",
                SecurityType::Cfd => "cfd",
                SecurityType::Crypto => "spot",
                SecurityType::FutureOption => "fut_opt",
                SecurityType::Index => "idx",
                SecurityType::IndexOption => "idx_opt",
            })
            .unwrap_or("");
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
                ("ast", asset_str.to_string()),
                ("sym", match xch {
                    Exchange::Binance => pair.to_string().replace('_', ""),
                    _ => pair.to_string(),
                }),
                ("dt", dt_par),
            ]),
        }
    }

    pub(crate) fn default_format(&self) -> DataFormat {
        match self {
            MarketEventDatasetType::OrderbooksByMinute
            | MarketEventDatasetType::OrderbooksBySecond
            | MarketEventDatasetType::OrderbooksRaw => DataFormat::Avro,
            MarketEventDatasetType::OrderbooksFlat => DataFormat::Csv,
            MarketEventDatasetType::Trades => DataFormat::Parquet,
        }
    }
}

#[derive(Debug, Deserialize, Clone, EnumString, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum DataFormat {
    #[strum(serialize = "avro")]
    Avro,
    #[strum(serialize = "parquet")]
    Parquet,
    #[strum(serialize = "csv")]
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
