use std::collections::HashSet;
use std::path::PathBuf;

use chrono::{Date, Duration, TimeZone, Utc};
use futures::StreamExt;

use strategy::coinnect::prelude::{Exchange, Pair};
use strategy::coinnect::types::MarketEventEnvelope;
use strategy::Channel;
use util::time::DateRange;

use crate::coinnect::broker::{AsyncBroker, ChannelMessageBroker};
use crate::error::*;
use crate::{csv_orderbooks_df, events_from_csv_orderbooks, events_from_orderbooks, raw_orderbooks_df,
            sampled_orderbooks_df};

pub struct Dataset {
    pub input_format: DatasetInputFormat,
    pub ds_type: DatasetType,
    pub base_dir: PathBuf,
    pub period: DateRange,
    pub input_sample_rate: Duration,
}

impl Dataset {
    pub async fn read_channels(&self, broker: ChannelMessageBroker<Channel, MarketEventEnvelope>) -> Result<()> {
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
            match self.ds_type {
                DatasetType::OrderbooksByMinute | DatasetType::OrderbooksBySecond => {
                    let input_format = self.input_format.to_string();
                    let event_stream = sampled_orderbooks_df(orderbook_partitions, input_format)
                        .map(events_from_orderbooks)
                        .flatten();
                    event_stream.for_each(|event| broker.broadcast(event)).await;
                }
                DatasetType::OrderbooksRaw => {
                    if let DatasetInputFormat::Csv = self.input_format {
                        let records = csv_orderbooks_df(orderbook_partitions).await?;
                        let event_stream = tokio_stream::iter(records).map(events_from_csv_orderbooks).flatten();
                        event_stream.for_each(|event| broker.broadcast(event)).await;
                    } else {
                        let records = raw_orderbooks_df(
                            orderbook_partitions,
                            self.input_sample_rate,
                            false,
                            &self.input_format.to_string(),
                        )
                        .await?;
                        let event_stream = tokio_stream::iter(records).map(events_from_orderbooks).flatten();
                        event_stream.for_each(|event| broker.broadcast(event)).await;
                    }
                }
                DatasetType::Trades => panic!("order books channel requires an order books dataset"),
            };
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
            DatasetType::OrderbooksByMinute => (base_dir, vec![
                ("xch", xch.to_string()),
                ("chan", "1mn_order_books".to_string()),
                ("dt", dt_par),
            ]),
            DatasetType::OrderbooksBySecond => (base_dir, vec![
                ("xch", xch.to_string()),
                ("chan", "1s_order_books".to_string()),
                ("dt", dt_par),
            ]),
            DatasetType::OrderbooksRaw => (base_dir.join(xch.to_string()).join("order_books"), vec![
                ("pr", pair.to_string()),
                ("dt", dt_par),
            ]),
            DatasetType::Trades => (base_dir.join(xch.to_string()).join("trades"), vec![
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
