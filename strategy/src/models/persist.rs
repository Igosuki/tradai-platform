use std::ops::Index;
use std::slice::SliceIndex;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use db::Storage;
use db::StorageExt;
use ext::ResultExt;
use util::time::now;

use crate::error::Result;
use crate::models::{TimedValue, TimedWindow, Window};

static MODELS_TABLE_NAME: &str = "models";

pub type ModelUpdateFn<T, A> = for<'a> fn(&'a mut T, A) -> &'a T;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelValue<T> {
    pub value: T,
    pub at: DateTime<Utc>,
}

impl<T> ModelValue<T> {
    pub fn new(value: T) -> Self { Self { value, at: now() } }
}

#[derive(Debug)]
pub struct PersistentModel<T> {
    pub last_model: Option<ModelValue<T>>,
    last_model_load_attempt: Option<DateTime<Utc>>,
    pub db: Arc<dyn Storage>,
    pub key: String,
    is_loaded: bool,
}

impl<T: Serialize + DeserializeOwned + Copy> PersistentModel<T> {
    /// # Panics
    ///
    /// if the table cannot be ensured
    pub fn new(db: Arc<dyn Storage>, key: &str, init: Option<ModelValue<T>>) -> Self {
        //let db = get_or_create(db_path, vec![MODELS_TABLE_NAME.to_string()]);
        db.ensure_table(MODELS_TABLE_NAME).unwrap();
        Self {
            db,
            key: key.to_string(),
            last_model: init,
            last_model_load_attempt: None,
            is_loaded: false,
        }
    }

    pub fn load(&mut self) -> crate::error::Result<()> {
        self.last_model_load_attempt = Some(Utc::now());
        let result = self.db.get(MODELS_TABLE_NAME, &self.key);
        if let Err(e) = result.map(|lmv| self.last_model = Some(lmv)) {
            match e {
                // Ignore not found since this simply means the model was never persisted
                db::Error::NotFound(_) => {}
                _ => return Err(e.into()),
            }
        }
        self.is_loaded = true;
        Ok(())
    }

    pub fn set_last_model(&mut self, new_model: T) { self.last_model = Some(ModelValue::new(new_model)); }

    pub fn update<A>(&mut self, update_fn: ModelUpdateFn<T, A>, args: A) -> Result<()> {
        if let Some(model) = self.last_model.as_mut() {
            (update_fn).call((&mut model.value, args));
            self.persist()?;
        }
        Ok(())
    }

    pub fn persist(&mut self) -> Result<()> { self.db.put(MODELS_TABLE_NAME, &self.key, &self.last_model).err_into() }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.last_model.as_ref().map(|m| m.at) }

    pub fn wipe(&mut self) -> Result<()> {
        self.db.delete(MODELS_TABLE_NAME, &self.key)?;
        self.last_model = None;
        Ok(())
    }

    pub fn has_model(&self) -> bool { self.last_model.is_some() }

    pub fn value(&self) -> Option<T> { self.last_model.as_ref().map(|s| s.value) }

    pub fn try_loading(&mut self) -> crate::error::Result<()> {
        if self.last_model_load_attempt.is_none() {
            self.load()?;
        }
        if self.is_loaded {
            Ok(())
        } else {
            Err(crate::error::Error::ModelLoadError(self.key.clone()))
        }
    }

    pub fn is_loaded(&self) -> bool { self.is_loaded }
}

// Fixed size vector backed by a database
#[derive(Debug)]
pub struct PersistentVec<T> {
    pub rows: Vec<TimedValue<T>>,
    db: Arc<dyn Storage>,
    max_size: usize,
    pub window_size: usize,
    key: String,
    last_load_attempt: Option<DateTime<Utc>>,
    is_loaded: bool,
}

impl<T, I: SliceIndex<[TimedValue<T>]>> Index<I> for PersistentVec<T> {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output { &self.rows[index] }
}

/// Values are written and sorted by time
impl<T: DeserializeOwned + Serialize> PersistentVec<T> {
    /// # Panics
    ///
    /// if the table cannot be ensured
    pub fn new(db: Arc<dyn Storage>, key: &str, max_size: usize, window_size: usize) -> Self {
        db.ensure_table(key).unwrap();
        assert!(max_size > window_size);
        Self {
            rows: vec![],
            db,
            max_size,
            window_size,
            key: key.to_string(),
            last_load_attempt: None,
            is_loaded: false,
        }
    }

    pub fn push(&mut self, row: T) {
        let i = Utc::now().timestamp_nanos();
        let k: &str = &i.to_string();
        if let Err(e) = self.db.put(&self.key, k, &row) {
            error!("Failed writing row : {:?}", e);
        }
        let timed_row = TimedValue(i, row);
        self.rows.push(timed_row);
        // Truncate the table by window_size once max_size is reached
        if let Err(e) = self.maybe_drain() {
            error!("Failed to delete range of rows : {:?}", e);
        }
    }

    pub fn push_all(&mut self, row: Vec<TimedValue<T>>) -> Result<()> {
        self.rows.extend(row.into_iter());
        self.rows.sort_by_key(|t| t.0);
        for row in &self.rows {
            let x: &str = &row.0.to_string();
            self.db.put(&self.key, x, &row.1)?;
        }
        self.maybe_drain()
    }

    fn maybe_drain(&mut self) -> Result<()> {
        if self.rows.len() > self.max_size {
            let mut drained = self.rows.drain(0..(self.max_size - self.window_size));
            let from = drained.next().unwrap();
            let to = drained.last().unwrap();
            self.db.delete_range(&self.key, from.0.to_string(), to.0.to_string())?;
        }
        Ok(())
    }

    pub fn window(&self) -> Window<T> { self.timed_window().map(|r| &r.1) }

    pub fn timed_window(&self) -> TimedWindow<'_, T> { self.rows.iter().rev().take(self.window_size).rev() }

    pub fn len(&self) -> usize { self.rows.len() }

    pub fn is_empty(&self) -> bool { self.rows.is_empty() }

    /// # Panics
    ///
    /// If record keys cannot be parsed as i64 timestamps
    pub fn load(&mut self) -> crate::error::Result<()> {
        self.rows = self
            .db
            .get_all(&self.key)?
            .into_iter()
            .map(|v| TimedValue(std::str::from_utf8(&*v.0).unwrap().parse::<i64>().unwrap(), v.1))
            .sorted_by_key(|t| t.0)
            .collect();
        self.last_load_attempt = Some(Utc::now());
        self.is_loaded = true;
        Ok(())
    }

    pub fn is_filled(&self) -> bool { self.len() > self.window_size }

    pub fn try_loading(&mut self) -> crate::error::Result<()> {
        if self.last_load_attempt.is_none() {
            self.load()?;
        }
        if self.is_loaded {
            Ok(())
        } else {
            Err(crate::error::Error::ModelLoadError(self.key.clone()))
        }
    }

    pub fn is_loaded(&self) -> bool { self.is_loaded }

    pub fn wipe(&mut self) -> Result<()> {
        self.db.delete_range(
            &self.key,
            // Beginning of 'time'
            Utc.timestamp_nanos(0).to_string(),
            Utc::now().timestamp_nanos().to_string(),
        )?;
        self.rows = vec![];
        Ok(())
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use test::Bencher;

    use chrono::{DateTime, Utc};
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};

    use db::{get_or_create, DbOptions};
    use trading::book::BookPosition;
    use util::test::test_dir;

    use crate::models::persist::PersistentVec;
    use crate::test_util::test_db;

    use super::ModelValue;
    use super::PersistentModel;

    #[derive(Debug, Clone, Serialize, Deserialize, Copy)]
    struct MockLinearModel;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let db = test_db();
        let mut table: PersistentModel<MockLinearModel> =
            PersistentModel::new(db, "default", Some(ModelValue::new(MockLinearModel {})));
        let _gen = Gen::new(500);
        b.iter(|| table.update(|m, _a| m, ()).unwrap());
        table.try_loading().unwrap();
    }

    #[test]
    fn test_save_load_model_is_sorted() {
        let id = "test_vec";
        let max_size = 11;
        let test_dir = test_dir();
        let options = &DbOptions::new(test_dir.as_ref());
        let window: Vec<TestRow> = {
            let db = get_or_create(options, "", vec![]);
            let mut table = PersistentVec::new(db.clone(), id, max_size, max_size / 2);
            let mut gen = Gen::new(500);
            for _ in 0..max_size {
                table.push(TestRow::arbitrary(&mut gen));
            }
            table.window().cloned().collect()
        };
        let db = get_or_create(options, "", vec![]);
        let mut table: PersistentVec<TestRow> = PersistentVec::new(db.clone(), id, max_size, max_size / 2);
        let load = table.load();
        assert!(load.is_ok(), "{:?}", load);
        let window_after_load: Vec<TestRow> = table.window().cloned().collect();
        assert_eq!(window, window_after_load);
    }
}
