#[macro_use]
extern crate serde_derive;

use std::ops::Sub;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, NaiveDate, TimeZone, Utc};
use config::{Config, ConfigError, File};
use datafusion::physical_plan::avro::AvroReadOptions;
use datafusion::prelude::ExecutionContext;
use parse_duration::parse;

use datafusion::arrow::array::{Array, ListArray, PrimitiveArray, StructArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use strategies::coinnect_types::{LiveEvent, LiveEventEnvelope, Orderbook};
use strategies::input::partition_path;
use strategies::order_manager::test_util::mock_manager;
use strategies::{Channel, DbOptions, Strategy, StrategySettings};
use util::date::{DateRange, DurationRangeType};
use util::test::test_dir;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("configuration error")]
    ConfError(#[from] ConfigError),
    #[error("datafusion error")]
    DataFusionError(#[from] DataFusionError),
    #[error("arrow error")]
    ArrowError(#[from] ArrowError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Period {
    Since { since: String },
    Interval { from: NaiveDate, to: Option<NaiveDate> },
}

impl Period {
    fn as_range(&self) -> DateRange {
        match self {
            Period::Since { since } => {
                let duration = Duration::from_std(parse(since).unwrap()).unwrap();
                let now = Utc::now();
                DateRange(now.sub(duration).date(), now.date(), DurationRangeType::Days, 1)
            }
            Period::Interval { from, to } => DateRange(
                Utc.from_utc_date(from),
                Utc.from_utc_date(&to.unwrap_or_else(|| Utc::now().naive_utc().date())),
                DurationRangeType::Days,
                1,
            ),
        }
    }
}

#[derive(Deserialize)]
pub struct BacktestConfig {
    db_path: Option<PathBuf>,
    strat: StrategySettings,
    fees: f64,
    period: Period,
    input_sample_rate: String,
    data_dir: PathBuf,
}

impl BacktestConfig {
    pub fn new(config_file_name: String) -> Result<Self> {
        let mut s = Config::new();

        s.merge(File::with_name(&config_file_name)).unwrap();

        // You may also programmatically change settings
        s.set("__config_file", config_file_name)?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into().map_err(|e| e.into())
    }

    pub fn sample_rate(&self) -> Duration { Duration::from_std(parse(&self.input_sample_rate).unwrap()).unwrap() }
}

pub struct Backtest {
    period: DateRange,
    strategy: Arc<Strategy>,
    data_dir: PathBuf,
    sample_rate: Duration,
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
        eprintln!("db_path = {:?}", db_path);
        let order_manager_addr = mock_manager(&db_path);

        let strategy = strategies::Strategy::new(
            &DbOptions::new(db_path),
            conf.fees,
            &conf.strat,
            Some(order_manager_addr),
        );
        Ok(Self {
            period: conf.period.as_range(),
            strategy: Arc::new(strategy),
            data_dir: conf.data_dir.clone(),
            sample_rate: conf.sample_rate(),
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
                    let records = read_order_books_df(partitions, self.sample_rate, false).await?;
                    for record_batch in records {
                        let sa: StructArray = record_batch.into();
                        let asks_col = sa
                            .column_by_name("asks")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap();
                        let bids_col = sa
                            .column_by_name("bids")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap();
                        let event_ms_col = sa
                            .column_by_name("event_ms")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
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
                            let i1 = event_ms_col.value(i);
                            let orderbook = Orderbook {
                                timestamp: i1,
                                pair: pair.clone(),
                                asks,
                                bids,
                                last_order_id: None,
                            };
                            live_events.push(LiveEventEnvelope {
                                xch: *xch,
                                e: LiveEvent::LiveOrderbook(orderbook),
                            });
                        }
                    }
                }
                Channel::Trades { .. } => {
                    panic!("cannot yet read from trades");
                }
                Channel::Orders { .. } => {
                    panic!("cannot yet read from orders");
                }
            }
        }
        for live_event in live_events {
            if let Err(e) = self.strategy.1.send(Arc::new(live_event)).await {
                println!("error sending event to strat : mailbox {}", e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        Ok(())
    }
}

async fn read_order_books_df(
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
        ctx.register_avro("order_books", &partition, AvroReadOptions::default())?;
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
