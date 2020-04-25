use chrono::{DateTime, Utc};
use coinnect_rt::types::{BigDecimalConv, Orderbook};
use db::Db;
use itertools::Itertools;
use math::iter::{CovarianceExt, MeanExt, VarianceExt};
use std::iter::{Rev, Take};
use std::slice::Iter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookPosition {
    pub mid: f64,
    // mid = (top_ask + top_bid) / 2, alias: crypto1_m
    pub ask: f64,
    // crypto_a
    ask_q: f64,
    // crypto_a_q
    pub bid: f64,
    // crypto_b
    bid_q: f64, // crypto_b_q
}

impl BookPosition {
    pub fn new(ask: f64, ask_q: f64, bid: f64, bid_q: f64) -> Self {
        Self {
            ask,
            ask_q,
            bid,
            bid_q,
            mid: Self::mid(ask, bid),
        }
    }

    fn mid(ask: f64, bid: f64) -> f64 {
        (ask + bid) / 2.0
    }

    pub fn from_book(t: Orderbook) -> Option<BookPosition> {
        let first_ask = t
            .asks
            .first()
            .map(|a| (a.0.as_f64().unwrap(), a.1.as_f64().unwrap()));
        let first_bid = t
            .bids
            .first()
            .map(|a| (a.0.as_f64().unwrap(), a.1.as_f64().unwrap()));
        match (first_ask, first_bid) {
            (Some(ask), Some(bid)) => return Some(BookPosition::new(ask.0, ask.1, bid.0, bid.1)),
            _ => return None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRow {
    pub time: DateTime<Utc>,
    pub left: BookPosition,
    // crypto_1
    pub right: BookPosition, // crypto_2
}

#[derive(Debug)]
pub struct DataTable {
    rows: Vec<DataRow>,
    window_size: usize,
    max_size: usize,
    db: Db,
    id: String,
    last_model: Option<LinearModelValue>,
    last_model_load_attempt: Option<DateTime<Utc>>,
}

const LINEAR_MODEL_KEY: &'static str = "linear_model";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinearModelValue {
    pub beta: f64,
    pub alpha: f64,
    pub at: DateTime<Utc>,
}

impl Default for DataTable {
    fn default() -> Self {
        DataTable {
            db: Db::new("default", "default".to_string()),
            id: "default".to_string(),
            window_size: 500,
            max_size: 1000,
            rows: Vec::new(),
            last_model: None,
            last_model_load_attempt: None,
        }
    }
}

impl DataTable {
    pub fn new(id: &str, db_path: &str, window_size: usize) -> Self {
        let db = Db::new(
            &format!("{}/naive_pair_trading_model_{}", db_path, id),
            id.to_string(),
        );
        DataTable {
            id: id.to_string(),
            rows: Vec::new(),
            window_size,
            max_size: window_size * 2, // Keep window_size * 8 elements
            db: db,
            last_model: None,
            last_model_load_attempt: None,
        }
    }

    pub fn update_model(&mut self) {
        let beta = self.beta();
        let alpha = self.alpha(beta);
        let now = Utc::now();
        let value = LinearModelValue {
            beta,
            alpha,
            at: now,
        };
        self.last_model = Some(value);
        self.db.put_json(LINEAR_MODEL_KEY, &self.last_model);
        self.db.delete_all("row");
        self.db
            .put_all_json("row", &self.current_window().rev().collect());
    }

    pub fn load_model(&mut self) {
        let lmv = self.db.read_json(LINEAR_MODEL_KEY);
        self.last_model = lmv;
        self.rows = self.db.read_json_vec("row");
        self.last_model_load_attempt = Some(Utc::now());
    }

    pub fn wipe_model(&mut self) {
        self.db.delete_all(LINEAR_MODEL_KEY);
        self.db.delete_all("row");
    }

    pub fn model(&self) -> Box<Option<LinearModelValue>> {
        Box::new(self.last_model.clone())
    }

    pub fn has_model(&self) -> bool {
        self.last_model.is_some()
    }

    pub fn try_loading_model(&mut self) -> bool {
        if self.last_model_load_attempt.is_some() || self.has_model() {
            return false;
        }
        self.load_model();
        self.last_model.is_some()
    }

    fn current_window(&self) -> Take<Rev<Iter<DataRow>>> {
        self.rows.iter().rev().take(self.window_size)
    }

    pub fn push(&mut self, row: &DataRow) {
        self.rows.push(row.clone());
        // Truncate the table by window_size once max_size is reached
        self.db
            .put_json(&format!("row{}", self.rows.len() - 1), row);
        if self.rows.len() > self.max_size {
            self.rows.drain(0..self.window_size);
        }
    }

    pub fn beta(&self) -> f64 {
        let (var, covar) = self.current_window().tee();
        let variance: f64 = var.map(|r| r.left.mid).variance();
        trace!("variance {:?}", variance);
        let covariance: f64 = covar
            .map(|r| (r.left.mid, r.right.mid))
            .covariance::<(f64, f64), f64>();
        trace!("covariance {:?}", covariance);
        let beta_val = covariance / variance;
        trace!("beta_val {:?}", beta_val);
        beta_val
    }

    pub fn alpha(&self, beta_val: f64) -> f64 {
        let (left, right) = self.current_window().tee();
        let mean_left: f64 = left.map(|l| l.left.mid).mean();
        trace!("mean left {:?}", mean_left);
        let mean_right: f64 = right.map(|l| l.right.mid).mean();
        trace!("mean right {:?}", mean_right);
        mean_right - beta_val * mean_left
    }

    pub fn predict(&self, alpha_val: f64, beta_val: f64, bp: &BookPosition) -> f64 {
        let p = alpha_val + beta_val * bp.mid;
        trace!("predict {:?}", p);
        p
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use crate::naive_pair_trading::data_table::{BookPosition, DataRow, DataTable};
    use db::Db;
    use tempfile::TempDir;

    use chrono::{TimeZone, Utc};
    use quickcheck::{Arbitrary, Gen, QuickCheck, StdThreadGen};
    use test::Bencher;

    impl Arbitrary for BookPosition {
        fn arbitrary<G: Gen>(g: &mut G) -> BookPosition {
            BookPosition {
                ask: f64::arbitrary(g),
                ask_q: f64::arbitrary(g),
                bid: f64::arbitrary(g),
                bid_q: f64::arbitrary(g),
                mid: f64::arbitrary(g),
            }
        }
    }

    impl Arbitrary for DataRow {
        fn arbitrary<G: Gen>(g: &mut G) -> DataRow {
            DataRow {
                time: Utc.timestamp_millis(f64::arbitrary(g) as i64),
                left: BookPosition::arbitrary(g),
                right: BookPosition::arbitrary(g),
            }
        }
    }

    fn test_db() -> Db {
        let tempdir = TempDir::new().unwrap();
        Db::new(tempdir.into_path().to_str().unwrap(), "temp".to_string())
    }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let mut table = DataTable {
            db: test_db(),
            id: "default".to_string(),
            window_size: 500,
            max_size: 1000,
            rows: Vec::new(),
            last_model: None,
            last_model_load_attempt: None,
        };
        let mut gen = StdThreadGen::new(500);
        for _ in 0..table.max_size {
            table.push(&DataRow::arbitrary(&mut gen))
        }
        b.iter(|| {
            table.update_model();
            table.load_model();
        });
    }
}
