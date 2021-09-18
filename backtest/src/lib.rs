#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::collections::BTreeMap;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration;
use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::ExecutionContext;
use serde::{ser::SerializeSeq, Serializer};

use strategies::coinnect_types::{LiveEvent, LiveEventEnvelope, Orderbook, Pair};
use strategies::input::partition_path;
use strategies::margin_interest_rates::test_util::mock_interest_rate_provider;
use strategies::order_manager::test_util::mock_manager;
use strategies::query::{DataQuery, DataResult};
use strategies::settings::StrategySettings;
use strategies::{Channel, DbOptions, Exchange, Strategy};
use util::date::DateRange;
use util::test::test_dir;

pub use crate::{config::*, error::*};

mod config;
mod error;

#[derive(Serialize, Deserialize, Default)]
struct BacktestReport {
    model_failures: u32,
}

pub struct Backtest {
    period: DateRange,
    strategy: Arc<Strategy>,
    data_dir: PathBuf,
    sample_rate: Duration,
    output_dir: PathBuf,
}

impl Backtest {
    pub fn try_new(conf: &BacktestConfig) -> Result<Self> {
        let db_path = conf
            .db_path
            .clone()
            .unwrap_or_else(|| test_dir().into_path())
            .into_os_string()
            .into_string()
            .unwrap();
        info!("db_path = {:?}", db_path);
        let output_path = conf.output_dir.clone().unwrap_or_else(|| {
            let mut p = test_dir().into_path();
            p.push("results");
            p
        });
        info!("output_path = {:?}", db_path);
        let order_manager_addr = mock_manager(&db_path);
        let margin_interest_rate_provider_addr = mock_interest_rate_provider(conf.strat.exchange());

        let strategy = if conf.use_generic {
            strategies::Strategy::new(
                &DbOptions::new(db_path),
                conf.fees,
                &StrategySettings::Generic(Box::new(conf.strat.clone())),
                Some(order_manager_addr),
                margin_interest_rate_provider_addr,
            )
        } else {
            strategies::Strategy::new(
                &DbOptions::new(db_path),
                conf.fees,
                &conf.strat,
                Some(order_manager_addr),
                margin_interest_rate_provider_addr,
            )
        };
        Ok(Self {
            period: conf.period.as_range(),
            strategy: Arc::new(strategy),
            data_dir: conf.data_dir.clone(),
            sample_rate: conf.sample_rate(),
            output_dir: output_path,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let _act = &self.strategy.as_ref().1;
        let chans = self.strategy.2.as_slice();
        //let mut heap = BinaryHeap::new();
        let period = self.period.clone();
        let mut live_events = vec![];
        for chan in chans {
            match chan {
                Channel::Orderbooks { xch, pair } => {
                    let mut partitions = vec![];
                    for date in period.clone() {
                        let partition = partition_path(
                            &xch.to_string(),
                            date.and_hms_milli(0, 0, 0, 0).timestamp_millis(),
                            "order_books",
                            pair.as_ref(),
                        );
                        if let Some(partition) = partition {
                            let mut partition_file = self.data_dir.clone();
                            partition_file.push(partition);
                            partitions.push(partition_file.as_path().to_string_lossy().to_string());
                        }
                    }
                    let records = avro_orderbooks_df(partitions, self.sample_rate, false).await?;
                    live_events.extend(events_from_grouped_arrays(*xch, pair.clone(), records))
                }
                Channel::Trades { .. } => {
                    panic!("cannot yet read from trades");
                }
                Channel::Orders { .. } => {
                    panic!("cannot yet read from orders");
                }
            }
        }
        let mut all_models: Vec<Vec<(String, Option<serde_json::Value>)>> = vec![];
        let mut report = BacktestReport::default();
        for live_event in live_events {
            self.strategy.1.do_send(Arc::new(live_event));
            match self.strategy.1.send(DataQuery::Models).await {
                Err(_) => log::error!("Mailbox error, strategy full"),
                Ok(Ok(Some(DataResult::Models(models)))) => all_models.push(models),
                _ => {
                    report.model_failures += 1;
                }
            }
            match self.strategy.1.send(DataQuery::Models).await {
                Err(_) => log::error!("Mailbox error, strategy full"),
                Ok(Ok(Some(DataResult::Models(models)))) => all_models.push(models),
                _ => {
                    report.model_failures += 1;
                }
            }
        }
        std::fs::create_dir_all(self.output_dir.clone())?;
        write_models(self.output_dir.clone(), all_models);
        write_report(self.output_dir.clone(), report);

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        Ok(())
    }
}

fn write_report(output_dir: PathBuf, report: BacktestReport) {
    let mut file = output_dir;
    file.push("report.json");
    let logs_f = std::fs::File::create(file).unwrap();
    let mut ser = serde_json::Serializer::new(BufWriter::new(logs_f));
    let mut seq = ser.serialize_seq(None).unwrap();
    seq.serialize_element(&report).unwrap();
    seq.end().unwrap();
}

fn write_models(output_dir: PathBuf, all_models: Vec<Vec<(String, Option<serde_json::Value>)>>) {
    let mut file = output_dir;
    file.push("models.json");
    let logs_f = std::fs::File::create(file).unwrap();
    let mut ser = serde_json::Serializer::new(BufWriter::new(logs_f));
    let mut seq = ser.serialize_seq(None).unwrap();
    for models in all_models {
        let obj: BTreeMap<String, Option<serde_json::Value>> = models.into_iter().collect();
        seq.serialize_element(&obj).unwrap();
    }
    seq.end().unwrap();
}

async fn avro_orderbooks_df(
    partitions: Vec<String>,
    sample_rate: Duration,
    order_book_split_cols: bool,
) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    dbg!(&partitions);
    let mut records = vec![];
    let order_book_selector = if order_book_split_cols {
        "asks[0][0] as a1, asks[0][1] as aq1, asks[1][0] as a2, asks[1][1] as aq2, asks[2][0] as a3, asks[2][1] as aq3, asks[3][0] as a4, asks[3][1] as aq4, asks[4][0] as a5, asks[4][1] as aq5, bids[0][0] as b1, bids[0][1] as bq1, bids[1][0] as b2, bids[1][1] as bq2, bids[2][0] as b3, bids[2][1] as bq3, bids[3][0] as b4, bids[3][1] as bq4, bids[4][0] as b5, bids[4][1] as bq5"
    } else {
        "asks, bids"
    };
    for partition in partitions {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE order_books STORED AS AVRO LOCATION '{partition}';",
            partition = &partition
        ))?;
        //ctx.register_avro("order_books", &partition, AvroReadOptions::default())?;
        let sql_query = format!(
            "select to_timestamp_millis(event_ms) as event_ms, {order_book_selector} from
   (select asks, bids, event_ms, ROW_NUMBER() OVER (PARTITION BY sample_time order by event_ms) as row_num
    FROM (select asks, bids, event_ms / {sample_rate} as sample_time, event_ms from order_books)) where row_num = 1;",
            sample_rate = sample_rate.num_milliseconds(),
            order_book_selector = order_book_selector
        );
        let df = ctx.sql(&sql_query)?;
        let results = df.collect().await?;
        records.extend_from_slice(results.as_slice());
    }
    Ok(records)
}

fn events_from_grouped_arrays(xchg: Exchange, pair: Pair, records: Vec<RecordBatch>) -> Vec<LiveEventEnvelope> {
    let mut live_events = vec![];
    for record_batch in records {
        let sa: StructArray = record_batch.into();
        let asks_col = get_col_as::<ListArray>(&sa, "asks");
        let bids_col = get_col_as::<ListArray>(&sa, "bids");
        let event_ms_col = get_col_as::<TimestampMillisecondArray>(&sa, "event_ms");
        for i in 0..sa.len() {
            let mut bids = vec![];
            for bid in bids_col
                .value(i)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .iter()
                .flatten()
            {
                let vals = bid
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap()
                    .values();
                bids.push((vals[0], vals[1]));
            }
            let mut asks = vec![];
            for ask in asks_col
                .value(i)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .iter()
                .flatten()
            {
                let vals = ask
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap()
                    .values();
                asks.push((vals[0], vals[1]));
            }
            let ts = event_ms_col.value(i);
            live_events.push(live_order_book(xchg, pair.clone(), ts, asks, bids));
        }
    }
    live_events
}

fn get_col_as<'a, T: 'static>(sa: &'a StructArray, name: &str) -> &'a T {
    sa.column_by_name(name).unwrap().as_any().downcast_ref::<T>().unwrap()
}

fn live_order_book(
    exchange: Exchange,
    pair: Pair,
    ts: i64,
    asks: Vec<(f64, f64)>,
    bids: Vec<(f64, f64)>,
) -> LiveEventEnvelope {
    let orderbook = Orderbook {
        timestamp: ts,
        pair,
        asks,
        bids,
        last_order_id: None,
    };
    LiveEventEnvelope {
        xch: exchange,
        e: LiveEvent::LiveOrderbook(orderbook),
    }
}
