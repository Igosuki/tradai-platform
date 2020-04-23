#[macro_use]
extern crate clap;

use chrono::{DateTime, Timelike};
use chrono::{Duration, TimeZone, Utc};
use clap::{App, Arg};
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;
use log::kv::Source;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::fs::File;
use std::ops::Sub;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;
use strategies::naive_pair_trading::data_table::DataRow;
use strategies::naive_pair_trading::input::{to_pos, CsvRecord};
use strategies::naive_pair_trading::Strategy;
use util::date::{DateRange, DurationRangeType};

fn main() {
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
    let left_pair = value_t_or_exit!(matches, "left-pair", String);
    let right_pair = value_t_or_exit!(matches, "right-pair", String);
    let data_dir = value_t_or_exit!(matches, "data-dir", String);
    let exchange = value_t_or_exit!(matches, "exchange", String);
    let db_storage_path = value_t_or_exit!(matches, "db-storage-path", String);
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
    let now = Instant::now();
    let (left_records, right_records) =
        strategies::naive_pair_trading::input::load_records_from_csv(
            &DateRange(midnight.date(), end.date(), DurationRangeType::Days, 1),
            &base_path,
            &left_pair,
            &right_pair,
            "/*.csv",
        );
    println!("Left records count : {}", left_records.len());
    println!("Right records count : {}", right_records.len());

    let mut data_table =
        Strategy::make_lm_table(&left_pair, &right_pair, &db_storage_path, window_size);
    let mut right_only: i32 = 0;
    let mut left_only: i32 = 0;
    let mut both: i32 = 0;
    let by_date = |i: &&CsvRecord, j: &&CsvRecord| i.event_ms.cmp(&j.event_ms);
    let left_records_sorted: Vec<&CsvRecord> = left_records.iter().sorted_by(by_date).collect();
    let right_records_sorted: Vec<&CsvRecord> = right_records.iter().sorted_by(by_date).collect();
    let zip = left_records
        .iter()
        .sorted_by(by_date)
        .merge_join_by(right_records.iter().sorted_by(by_date), by_date)
        .map(|either| match either {
            Left(l) => {
                left_only += 1;
                None
            }
            Right(r) => {
                right_only += 1;
                None
            }
            Both(l, r) => {
                both += 1;
                Some((l, r))
            }
        });
    zip.while_some().for_each(|(l, r)| {
        let row_time = l.event_ms;
        if l.event_ms != r.event_ms {
            panic!(format!(
                "left and right time not aligned in stream !! {} {}",
                l.event_ms, r.event_ms
            ));
        }
        data_table.push(&DataRow {
            time: row_time,
            left: to_pos(l),
            right: to_pos(r),
        });
    });
    println!("Saving Model...");
    data_table.update_model();
    data_table.load_model();
    assert!(data_table.has_model());
    println!("{:?}", data_table.model());
    println!("Execution took {} seconds", now.elapsed().as_secs());
}
