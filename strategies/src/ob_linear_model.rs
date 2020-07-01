use chrono::{DateTime, Utc};
use coinnect_rt::types::Orderbook;
use db::{DataStoreError, Db};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::iter::{Rev, Take};
use std::slice::Iter;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataTableError {
    #[error("at least one bid expected")]
    MissingBids,
    #[error("at least one ask expected")]
    MissingAsks,
}

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
    pub fn new(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> Self {
        let first_ask = asks[0];
        let first_bid = bids[0];
        Self {
            ask: first_ask.0,
            ask_q: first_ask.1,
            bid: first_bid.0,
            bid_q: first_bid.1,
            mid: Self::mid(asks, bids),
        }
    }

    fn mid(asks: &[(f64, f64)], bids: &[(f64, f64)]) -> f64 {
        let (asks_iter, asks_iter2) = asks.iter().tee();
        let (bids_iter, bids_iter2) = bids.iter().tee();
        (asks_iter.map(|a| a.0 * a.1).sum::<f64>() + bids_iter.map(|b| b.0 * b.1).sum::<f64>())
            / asks_iter2.interleave(bids_iter2).map(|t| t.1).sum::<f64>()
    }
}

impl TryFrom<Orderbook> for BookPosition {
    type Error = DataTableError;

    fn try_from(t: Orderbook) -> Result<Self, Self::Error> {
        if t.asks.is_empty() {
            return Err(DataTableError::MissingAsks);
        }
        if t.bids.is_empty() {
            return Err(DataTableError::MissingBids);
        }
        Ok(BookPosition::new(&t.asks, &t.bids))
    }
}

type BetaFn<T> = Fn(&LinearModelTable<T>) -> f64 + Send + 'static;
type AlphaFn<T> = Fn(&LinearModelTable<T>, f64) -> f64 + Send + 'static;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct LinearModelTable<T> {
    rows: Vec<T>,
    window_size: usize,
    max_size: usize,
    db: Db,
    id: String,
    last_model: Option<LinearModelValue>,
    last_model_load_attempt: Option<DateTime<Utc>>,
    #[derivative(Debug = "ignore")]
    beta_fn: Box<BetaFn<T>>,
    #[derivative(Debug = "ignore")]
    alpha_fn: Box<AlphaFn<T>>,
}

static LINEAR_MODEL_KEY: &str = "linear_model";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinearModelValue {
    pub beta: f64,
    pub alpha: f64,
    pub at: DateTime<Utc>,
}

pub trait LinearModel<T>: Debug + Send {
    fn beta(&self, i: impl Iterator<Item = T>) -> f64;
    fn alpha(&self, b: f64, i: impl Iterator<Item = T>) -> f64;
}

impl<T: Serialize + DeserializeOwned + Clone> LinearModelTable<T> {
    pub fn new(
        id: &str,
        db_path: &str,
        window_size: usize,
        beta_fn: Box<BetaFn<T>>,
        alpha_fn: Box<AlphaFn<T>>,
    ) -> Self {
        let db = Db::new(&format!("{}/model_{}", db_path, id), id.to_string());
        Self {
            id: id.to_string(),
            rows: Vec::new(),
            window_size,
            max_size: window_size * 2, // Keep window_size * 2 elements
            db,
            last_model: None,
            last_model_load_attempt: None,
            beta_fn,
            alpha_fn,
        }
    }

    pub fn update_model(&mut self) -> Result<(), DataStoreError> {
        let beta = self.beta();
        let alpha = self.alpha(beta);
        let now = Utc::now();
        let value = LinearModelValue {
            beta,
            alpha,
            at: now,
        };
        self.last_model = Some(value);
        self.db.put_json(LINEAR_MODEL_KEY, &self.last_model)?;
        self.db.delete_all("row")?;
        let x: Vec<&T> = self.current_window().rev().collect();
        self.db.put_all_json("row", &x)?;
        Ok(())
    }

    pub fn load_model(&mut self) {
        let lmv = self.db.read_json(LINEAR_MODEL_KEY);
        self.last_model = lmv;
        self.rows = self.db.read_json_vec("row");
        self.last_model_load_attempt = Some(Utc::now());
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> {
        self.last_model.as_ref().map(|m| m.at)
    }

    pub fn wipe_model(&mut self) -> Result<(), DataStoreError> {
        self.db.delete_all(LINEAR_MODEL_KEY)?;
        self.db.delete_all("row")?;
        Ok(())
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

    pub(crate) fn current_window(&self) -> Take<Rev<Iter<T>>> {
        self.rows.iter().rev().take(self.window_size)
    }

    pub fn push(&mut self, row: &T) {
        self.rows.push(row.clone());
        // Truncate the table by window_size once max_size is reached
        if let Err(e) = self
            .db
            .put_json(&format!("row{}", self.rows.len() - 1), row)
        {
            error!("Failed writing row : {:?}", e);
        }
        if self.rows.len() > self.max_size {
            self.rows.drain(0..self.window_size);
        }
    }

    pub fn beta(&self) -> f64 {
        (self.beta_fn)(&self)
    }

    pub fn alpha(&self, beta_val: f64) -> f64 {
        (self.alpha_fn)(&self, beta_val)
    }

    pub fn predict(&self, alpha_val: f64, beta_val: f64, bp: &BookPosition) -> f64 {
        let p = alpha_val + beta_val * bp.mid;
        trace!("predict {}", p);
        p
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use db::Db;
    use tempfile::TempDir;

    use crate::ob_linear_model::{BookPosition, LinearModelTable};
    use chrono::{DateTime, TimeZone, Utc};
    use quickcheck::{Arbitrary, Gen, StdThreadGen};
    use test::Bencher;

    #[derive(Debug)]
    struct MockLinearModel;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestRow {
        pub time: DateTime<Utc>,
        pub pos: BookPosition, // crypto_1
    }

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

    impl Arbitrary for TestRow {
        fn arbitrary<G: Gen>(g: &mut G) -> TestRow {
            TestRow {
                time: Utc.timestamp_millis(f64::arbitrary(g) as i64),
                pos: BookPosition::arbitrary(g),
            }
        }
    }

    fn test_db() -> Db {
        let tempdir = TempDir::new().unwrap();
        Db::new(tempdir.into_path().to_str().unwrap(), "temp".to_string())
    }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let mut table: LinearModelTable<TestRow> = LinearModelTable {
            db: test_db(),
            id: "default".to_string(),
            window_size: 500,
            max_size: 1000,
            rows: Vec::new(),
            last_model: None,
            last_model_load_attempt: None,
            beta_fn: Box::new(|lm| lm.current_window().map(|t| t.pos.mid).sum::<f64>()),
            alpha_fn: Box::new(|lm, beta| {
                lm.current_window().map(|t| t.pos.mid).sum::<f64>() * beta
            }),
        };
        let mut gen = StdThreadGen::new(500);
        for _ in 0..table.max_size {
            table.push(&TestRow::arbitrary(&mut gen))
        }
        b.iter(|| {
            table.update_model().unwrap();
            table.load_model();
        });
    }
}
