#[macro_use]
extern crate clap;

use chrono::Timelike;
use chrono::{Duration, TimeZone, Utc};
use clap::{App, Arg};
use itertools::Itertools;
use log::kv::Source;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::fs::File;
use std::ops::Sub;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;
use strategies::naive_pair_trading::input::to_pos;
use strategies::naive_pair_trading::{DataRow, DataTable};
use util::date::{DateRange, DurationRangeType};

fn main() {
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
        .get_matches();
    let left_pair = value_t_or_exit!(matches, "left-pair", String);
    let right_pair = value_t_or_exit!(matches, "right-pair", String);
    let data_dir = value_t_or_exit!(matches, "data-dir", String);
    let exchange = value_t_or_exit!(matches, "exchange", String);
    let channel = "order_books";

    let base_path = Path::new(&data_dir);
    let sample_freq = Duration::minutes(1);
    let window_size = 500;
    let now = Utc::now();

    let midnight = now.date().and_hms(0, 0, 0);
    let lower_date_bound = now.sub(sample_freq * window_size);
    let end = if lower_date_bound < midnight {
        lower_date_bound.date().and_hms(0, 0, 0)
    } else {
        midnight
    };

    let base_path = Path::new(&data_dir).join(exchange).join(channel);

    Utc.timestamp(0, 0);
    let now = Instant::now();
    let (left_records, right_records) =
        strategies::naive_pair_trading::input::load_records_from_csv(
            &DateRange(midnight.date(), end.date(), DurationRangeType::Days, 1),
            &base_path,
            &left_pair,
            &right_pair,
        );
    let mut data_table = DataTable::new("dt", "data", 500);
    let zip = left_records.iter().zip(right_records.iter());
    println!("Left records count : {}", left_records.len());
    println!("Right records count : {}", right_records.len());
    zip.for_each(|(l, r)| {
        let row_time = l.hourofday;
        data_table.push(&DataRow {
            time: row_time,
            left: to_pos(l),
            right: to_pos(r),
        });
    });
    println!("{}", data_table.size());
    println!("{}", data_table.beta());
    println!("Execution took {} seconds", now.elapsed().as_secs());
}
