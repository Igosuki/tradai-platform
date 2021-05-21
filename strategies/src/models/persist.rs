use chrono::{Utc, DateTime};
use db::{DataStoreError, Db};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::iter::{Take, Rev};
use std::slice::Iter;

static LAST_MODEL_KEY: &str = "last_model";

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
    db: Db,
}

impl<T: DeserializeOwned + Serialize + Clone> PersistentModel<T> {
    pub fn new(db_path: &str, name: String, init: Option<ModelValue<T>>) -> Self {
        let db = Db::new(&format!("{}/model_{}", db_path, name), name);
        Self {
            db,
            last_model: init,
            last_model_load_attempt: None,
        }
    }

    pub fn load_model(&mut self) {
        let lmv = self.db.read_json(LAST_MODEL_KEY);
        self.last_model = lmv;
        self.last_model_load_attempt = Some(Utc::now());
    }

    fn set_last_model(&mut self, new_model: T) {
        self.last_model = Some(ModelValue {
            value: new_model,
            at: Utc::now(),
        });
    }

    pub fn update_model<A>(&mut self, update_fn: ModelUpdateFn<T, A>, args: A) -> Result<(), DataStoreError> {
        if let Some(model) = &self.last_model {
            let new_model_value = (update_fn).call((&model.value, args));
            self.set_last_model(new_model_value);
            self.db.put_json(LAST_MODEL_KEY, &self.last_model)?;
        }
        Ok(())
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> {
        self.last_model.as_ref().map(|m| m.at)
    }

    #[allow(dead_code)]
    pub fn wipe_model(&mut self) -> Result<(), DataStoreError> {
        self.db.delete_all(LAST_MODEL_KEY)?;
        Ok(())
    }

    pub fn has_model(&self) -> bool {
        self.last_model.is_some()
    }

    pub fn model(&self) -> Option<ModelValue<T>> {
        self.last_model.clone()
    }

    pub fn value(&self) -> Option<T> {
        self.last_model.clone().map(|s| s.value)
    }

    pub fn try_loading_model(&mut self) -> bool {
        if self.last_model_load_attempt.is_some() || self.has_model() {
            return false;
        }
        self.load_model();
        self.last_model.is_some()
    }
}

//pub type Window<'a, T> = impl Iterator<Item = &'a T>;
pub type Window<'a, T> = Take<Rev<Iter<'a, T>>>;

// Fixed size vector backed by a database
#[derive(Debug)]
pub struct PersistentVec<T> {
    pub rows: Vec<T>,
    db: Db,
    max_size: usize,
    pub window_size: usize,
}

const ROW_KEY: &str = "row";

impl<T: DeserializeOwned + Serialize + Clone> PersistentVec<T> {
    pub fn new(db_path: &str, name: String, max_size: usize, window_size: usize) -> Self {
        Self {
            rows: vec![],
            db: Db::new(db_path, name),
            max_size,
            window_size,
        }
    }

    pub fn push(&mut self, row: &T) {
        self.rows.push(row.clone());
        // Truncate the table by window_size once max_size is reached
        if let Err(e) = self
            .db
            .put_b(&format!("{}{}", ROW_KEY, self.rows.len() - 1), row)
        {
            error!("Failed writing row : {:?}", e);
        }
        if self.rows.len() > self.max_size {
            self.rows.drain(0..self.window_size);
            if let Err(e) = self.db.replace_all(ROW_KEY, self.rows.as_ref()) {
                error!("Failed to replace rows : {:?}", e);
            }
        }
    }

    pub(crate) fn window(&self) -> Window<T> {
        self.rows.iter().rev().take(self.window_size)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn load(&mut self) {
        self.rows = self.db.read_json_vec(ROW_KEY);
    }

    pub fn is_filled(&self) -> bool {
        self.len() > self.window_size
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use tempfile::TempDir;

    use crate::types::BookPosition;
    use super::ModelValue;
    use chrono::{DateTime, Utc};
    use quickcheck::{Arbitrary, Gen};
    use test::Bencher;
    use fake::Fake;
    use super::PersistentModel;

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
                value: MockLinearModel{},
                at: Utc::now(),
            })
        );
        let _gen = Gen::new(500);
        b.iter(|| table.update_model(|m, _a| m.clone(), ()).unwrap());
        table.load_model();
    }
}
