use chrono::{DateTime, Utc};
use db::{DataStoreError, Db};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::iter::{Rev, Take};
use std::slice::Iter;

type WindowFn<T> = dyn Fn(&WindowTable<T>) -> f64 + Send + 'static;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct WindowTable<T> {
    rows: Vec<T>,
    pub window_size: usize,
    max_size: usize,
    db: Db,
    id: String,
    last_model: Option<WindowModelValue>,
    last_model_load_attempt: Option<DateTime<Utc>>,
    #[derivative(Debug = "ignore")]
    window_fn: Box<WindowFn<T>>,
}

static LAST_MODEL_KEY: &str = "last_model";
static ROW_KEY: &str = "row";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowModelValue {
    pub value: f64,
    pub at: DateTime<Utc>,
}

impl<T: Serialize + DeserializeOwned + Clone> WindowTable<T> {
    pub fn new(
        id: &str,
        db_path: &str,
        window_size: usize,
        max_size_o: Option<usize>,
        window_fn: Box<WindowFn<T>>,
    ) -> Self {
        let db = Db::new(&format!("{}/model_{}", db_path, id), id.to_string());
        let max_size = max_size_o.unwrap_or_else(|| 2 * window_size);
        Self {
            id: id.to_string(),
            rows: Vec::new(),
            window_size,
            max_size, // Keep max_size elements
            db,
            last_model: None,
            last_model_load_attempt: None,
            window_fn,
        }
    }

    pub fn update_model(&mut self) -> Result<(), DataStoreError> {
        let value = (self.window_fn)(&self);
        let now = Utc::now();
        let value = WindowModelValue { value, at: now };
        self.last_model = Some(value);
        self.db.put_json(LAST_MODEL_KEY, &self.last_model)?;
        Ok(())
    }

    pub fn load_model(&mut self) {
        let lmv = self.db.read_json(LAST_MODEL_KEY);
        self.last_model = lmv;
        self.rows = self.db.read_json_vec(ROW_KEY);
        self.last_model_load_attempt = Some(Utc::now());
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> {
        self.last_model.as_ref().map(|m| m.at)
    }

    pub fn wipe_model(&mut self) -> Result<(), DataStoreError> {
        self.db.delete_all(LAST_MODEL_KEY)?;
        self.db.delete_all(ROW_KEY)?;
        Ok(())
    }

    pub fn model(&self) -> Box<Option<WindowModelValue>> {
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

    pub(crate) fn window(&self, window_size: usize) -> Take<Rev<Iter<T>>> {
        self.rows.iter().rev().take(window_size)
    }

    pub fn push(&mut self, row: &T) {
        self.rows.push(row.clone());
        // Truncate the table by window_size once max_size is reached
        if let Err(e) = self
            .db
            .put_json(&format!("{}{}", ROW_KEY, self.rows.len() - 1), row)
        {
            error!("Failed writing row : {:?}", e);
        }
        if self.rows.len() > self.max_size {
            self.rows.drain(0..self.window_size);
            if let Err(e) = self.db.delete_all(ROW_KEY) {
                error!("Failed to delete rows : {:?}", e);
            } else {
                let x: Vec<&T> = self.window(self.window_size).rev().collect();
                if let Err(e) = self.db.put_all_json(ROW_KEY, &x) {
                    error!("Failed to write all rows : {:?}", e);
                }
            }
        }
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
    use crate::ob_double_window_model::WindowTable;
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
        let mut table: WindowTable<TestRow> = WindowTable {
            db: test_db(),
            id: "default".to_string(),
            window_size: 1000,
            max_size: 2000,
            rows: Vec::new(),
            last_model: None,
            last_model_load_attempt: None,
            window_fn: Box::new(|lm| lm.window(lm.window_size).map(|t| t.pos.mid).sum::<f64>()),
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
