#[macro_use]
extern crate clap;

use avro_rs::{from_value, Reader};
use chrono::Timelike;
use chrono::{Duration, TimeZone, Utc};
use clap::{App, Arg};
use models::Orderbook;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::fs::File;
use std::ops::Sub;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

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

    let mut dates = vec![now];
    let midnight = now.date().and_hms(0, 0, 0);
    let lower_date_bound = now.sub(sample_freq * window_size);
    println!("{} {} {}", now, midnight, lower_date_bound);
    if lower_date_bound < midnight {
        dates.push(lower_date_bound.date().and_hms(0, 0, 0));
    }
    let left_paths: Vec<PathBuf> = dates
        .iter()
        .map(|d| {
            model_loader::input::partition_path(
                &exchange,
                d.timestamp_millis(),
                channel,
                &left_pair,
            )
            .unwrap()
        })
        .map(|p| base_path.join(p))
        .collect();
    let right_paths: Vec<PathBuf> = dates
        .iter()
        .map(|d| {
            model_loader::input::partition_path(
                &exchange,
                d.timestamp_millis(),
                channel,
                &right_pair,
            )
            .unwrap()
        })
        .map(|p| base_path.join(p))
        .collect();
    Utc.timestamp(0, 0);
    println!("{:?} {:?}", left_paths, right_paths);
    let now = Instant::now();
    let mut sum = 0 as u64;
    for path in vec![left_paths, right_paths].iter().flatten() {}
    println!("Counted {} records", sum);
    println!("Execution took {} seconds", now.elapsed().as_secs());
}
