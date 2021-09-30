#[macro_use]
extern crate clap;

use std::ops::Sub;
use std::path::Path;

use chrono::{Duration, TimeZone, Utc};
use clap::{App, Arg};
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;

use db::{get_or_create, DbOptions};
use strategies::input::CsvRecord;
use strategies::naive_pair_trading::covar_model::DataRow;
use strategies::naive_pair_trading::NaiveTradingStrategy;
use strategies::Model;
use util::date::{DateRange, DurationRangeType};

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let matches = App::new("Trader Naive Model Loader")
        .version("1.0")
        .arg(
            Arg::with_name("left-pair")
                .long("left-pair")
                .help("the left pair")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("right-pair")
                .long("right-pair")
                .help("the right pair")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .help("the avro data directory")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("exchange")
                .long("exchange")
                .help("the exchange to load data for")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-storage-path")
                .long("db-storage-path")
                .help("the internal database storage path")
                .required(true)
                .takes_value(true),
        )
        .get_matches();
    let left_pair = value_t!(matches, "left-pair", String).unwrap_or_else(|e| e.exit());
    let right_pair = value_t!(matches, "right-pair", String).unwrap_or_else(|e| e.exit());
    let data_dir = value_t!(matches, "data-dir", String).unwrap_or_else(|e| e.exit());
    let exchange = value_t!(matches, "exchange", String).unwrap_or_else(|e| e.exit());
    let db_storage_path = value_t!(matches, "db-storage-path", String).unwrap_or_else(|e| e.exit());
    let channel = "order_books";

    let base_path = Path::new(&data_dir).join(exchange).join(channel);
    let sample_freq = Duration::minutes(1);
    let window_size = 500;
    let now = Utc::now();

    let midnight = now.date().and_hms(0, 0, 0);
    let lower_date_bound = now.sub(sample_freq * window_size as i32);
    let end = if lower_date_bound < midnight {
        lower_date_bound.date().and_hms(0, 0, 0)
    } else {
        midnight
    };

    Utc.timestamp(0, 0);
    let records = strategies::input::load_records_from_csv(
        &DateRange(midnight.date(), end.date(), DurationRangeType::Days, 1),
        &base_path,
        vec![left_pair.clone(), right_pair.clone()],
        "/*.csv",
    );
    let left_records = &records[0];
    println!("Left records count : {}", left_records.len());
    let right_records = &records[1];
    println!("Right records count : {}", right_records.len());

    let db = get_or_create(&DbOptions::new(db_storage_path), "", vec![]);
    let mut data_table = NaiveTradingStrategy::make_lm_table(&left_pair, &right_pair, db, window_size);
    let mut right_only: i32 = 0;
    let mut left_only: i32 = 0;
    let mut both: i32 = 0;
    let by_date = |i: &&CsvRecord, j: &&CsvRecord| i.event_ms.cmp(&j.event_ms);
    let zip: Vec<(&CsvRecord, &CsvRecord)> = left_records
        .iter()
        .sorted_by(by_date)
        .merge_join_by(right_records.iter().sorted_by(by_date), by_date)
        .map(|either| match either {
            Left(_l) => {
                left_only += 1;
                None
            }
            Right(_r) => {
                right_only += 1;
                None
            }
            Both(l, r) => {
                both += 1;
                Some((l, r))
            }
        })
        .while_some()
        .collect();
    println!("{:?} \n {:?}", &zip.first(), &zip.last());
    zip.iter().rev().take(window_size).for_each(|(l, r)| {
        let row_time = l.event_ms;
        if l.event_ms != r.event_ms {
            panic!(
                "{}",
                format!(
                    "left and right time not aligned in stream !! {} {}",
                    l.event_ms, r.event_ms
                )
            );
        }
        data_table.push(&DataRow {
            time: row_time,
            left: (*l).clone().into(),
            right: (*r).clone().into(),
        });
    });
    println!("Saving Model...");
    data_table.update_model()?;
    data_table.try_load()?;
    assert!(data_table.has_model());
    println!("{:?}", data_table.model().unwrap());
    Ok(())
}
