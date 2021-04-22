use crate::model::BookPosition;
use chrono::{DateTime, Utc};
use db::{DataStoreError, Db};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::iter::{Rev, Take};
use std::slice::Iter;

type BetaFn<T> = dyn Fn(&LinearModelTable<T>) -> f64 + Send + 'static;
type AlphaFn<T> = dyn Fn(&LinearModelTable<T>, f64) -> f64 + Send + 'static;

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
        let _lmv_some = lmv.is_some();
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

    use crate::model::BookPosition;
    use crate::ob_linear_model::LinearModelTable;
    use chrono::{DateTime, TimeZone, Utc};
    use quickcheck::{Arbitrary, Gen};
    use test::Bencher;

    #[derive(Debug)]
    struct MockLinearModel;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestRow {
        pub time: DateTime<Utc>,
        pub pos: BookPosition, // crypto_1
    }

    impl Arbitrary for TestRow {
        fn arbitrary(g: &mut Gen) -> TestRow {
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
        let mut gen = Gen::new(500);
        for _ in 0..table.max_size {
            table.push(&TestRow::arbitrary(&mut gen))
        }
        b.iter(|| {
            table.update_model().unwrap();
            table.load_model();
        });
    }
}
