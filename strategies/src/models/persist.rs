use chrono::{DateTime, Utc};
use db::StorageBincodeExt;
use db::{get_or_create, Storage};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::Path;

static MODELS_TABLE_NAME: &str = "models";

type ModelUpdateFn<T, A> = fn(&T, A) -> T;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelValue<T> {
    pub value: T,
    pub at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct PersistentModel<T> {
    last_model: Option<ModelValue<T>>,
    last_model_load_attempt: Option<DateTime<Utc>>,
    db: Box<dyn Storage>,
    key: String,
}

impl<T: DeserializeOwned + Serialize + Clone> PersistentModel<T> {
    pub fn new<S: AsRef<Path>>(db_path: S, name: String, init: Option<ModelValue<T>>) -> Self {
        let db = get_or_create(db_path, vec![MODELS_TABLE_NAME.to_string()]);
        Self {
            db,
            key: format!("model_{}", name),
            last_model: init,
            last_model_load_attempt: None,
        }
    }

    pub fn load_model(&mut self) {
        if let Ok(lmv) = self.db.get(MODELS_TABLE_NAME, &self.key) {
            self.last_model = Some(lmv);
        }
        self.last_model_load_attempt = Some(Utc::now());
    }

    fn set_last_model(&mut self, new_model: T) {
        self.last_model = Some(ModelValue {
            value: new_model,
            at: Utc::now(),
        });
    }

    pub fn update_model<A>(&mut self, update_fn: ModelUpdateFn<T, A>, args: A) -> Result<(), db::Error> {
        if let Some(model) = &self.last_model {
            let new_model_value = (update_fn).call((&model.value, args));
            self.set_last_model(new_model_value);
            self.db.put(MODELS_TABLE_NAME, &self.key, &self.last_model)?;
        }
        Ok(())
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.last_model.as_ref().map(|m| m.at) }

    #[allow(dead_code)]
    pub fn wipe_model(&mut self) -> Result<(), db::Error> {
        self.db.delete(MODELS_TABLE_NAME, &self.key)?;
        Ok(())
    }

    pub fn has_model(&self) -> bool { self.last_model.is_some() }

    pub fn model(&self) -> Option<ModelValue<T>> { self.last_model.clone() }

    pub fn value(&self) -> Option<T> { self.last_model.clone().map(|s| s.value) }

    pub fn try_loading_model(&mut self) -> bool {
        if self.last_model_load_attempt.is_some() || self.has_model() {
            return false;
        }
        self.load_model();
        self.last_model.is_some()
    }
}

pub type Window<'a, T> = impl Iterator<Item = &'a T> + Clone;
//pub type Window<'a, T> = Take<Rev<Map<Iter<'a, T>>>>;

#[derive(Debug)]
pub struct TimedValue<T>(i64, T);

// Fixed size vector backed by a database
#[derive(Debug)]
pub struct PersistentVec<T> {
    pub rows: Vec<TimedValue<T>>,
    db: Box<dyn Storage>,
    max_size: usize,
    pub window_size: usize,
}

const ROWS_TABLE_NAME: &str = "rows";

impl<T: DeserializeOwned + Serialize + Clone> PersistentVec<T> {
    pub fn new(db_path: &str, name: String, max_size: usize, window_size: usize) -> Self {
        let db = get_or_create(&format!("{}/{}", db_path, name), vec![ROWS_TABLE_NAME.to_string()]);
        Self {
            rows: vec![],
            db,
            max_size,
            window_size,
        }
    }

    pub fn push(&mut self, row: &T) {
        let timed_row = TimedValue(Utc::now().timestamp_nanos(), row.clone());
        let x: &str = &timed_row.0.to_string();
        self.rows.push(timed_row);
        // Truncate the table by window_size once max_size is reached
        if let Err(e) = self.db.put(ROWS_TABLE_NAME, x, row) {
            error!("Failed writing row : {:?}", e);
        }
        if self.rows.len() > self.max_size {
            let mut drained = self.rows.drain(0..self.window_size);
            let from = drained.next().unwrap();
            let to = drained.last().unwrap();
            if let Err(e) = self
                .db
                .delete_range(ROWS_TABLE_NAME, from.0.to_string(), to.0.to_string())
            {
                error!("Failed to delete range of rows : {:?}", e);
            }
        }
    }

    pub(crate) fn window(&self) -> Window<T> { self.rows.iter().map(|r| &r.1).rev().take(self.window_size) }

    pub fn len(&self) -> usize { self.rows.len() }

    pub fn load(&mut self) {
        // self.rows = self.db.get_all(ROWS_TABLE_NAME);
    }

    pub fn is_filled(&self) -> bool { self.len() > self.window_size }
}

#[cfg(test)]
mod test {
    extern crate test;

    use tempfile::TempDir;

    use super::ModelValue;
    use super::PersistentModel;
    use crate::types::BookPosition;
    use chrono::{DateTime, Utc};
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};
    use test::Bencher;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct MockLinearModel;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestRow {
        pub time: DateTime<Utc>,
        pub pos: BookPosition, // crypto_1
    }

    impl Arbitrary for TestRow {
        fn arbitrary(g: &mut Gen) -> TestRow {
            let time: chrono::DateTime<Utc> = fake::faker::chrono::en::DateTime().fake();
            TestRow {
                time,
                pos: BookPosition::arbitrary(g),
            }
        }
    }

    // fn test_db() -> Db {
    //     let tempdir = TempDir::new().unwrap();
    //     Db::new(tempdir.into_path().to_str().unwrap(), "temp".to_string())
    // }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let tempdir = TempDir::new().unwrap();
        let mut table: PersistentModel<MockLinearModel> = PersistentModel::new(
            tempdir.into_path().to_str().unwrap(),
            "default".to_string(),
            Some(ModelValue {
                value: MockLinearModel {},
                at: Utc::now(),
            }),
        );
        let _gen = Gen::new(500);
        b.iter(|| table.update_model(|m, _a| m.clone(), ()).unwrap());
        table.load_model();
    }
}
