use chrono::{DateTime, Utc};
use db::{DataStoreError, Db};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

type ModelUpdateFn<T, R> = dyn Fn(&mut T, &R) -> () + Send + 'static;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct IndicatorModel<T, R> {
    db: Db,
    id: String,
    last_model: Option<ModelValue<T>>,
    last_model_load_attempt: Option<DateTime<Utc>>,
    #[derivative(Debug = "ignore")]
    update_fn: Box<ModelUpdateFn<T, R>>,
}

static LAST_MODEL_KEY: &str = "last_model";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelValue<T> {
    pub value: T,
    pub at: DateTime<Utc>,
}

impl<T: Serialize + DeserializeOwned + Clone, R> IndicatorModel<T, R> {
    pub fn new(
        id: &str,
        db_path: &str,
        initial_value: T,
        update_fn: Box<ModelUpdateFn<T, R>>,
    ) -> Self {
        let db = Db::new(&format!("{}/model_{}", db_path, id), id.to_string());
        Self {
            id: id.to_string(),
            db,
            last_model: Some(ModelValue {
                value: initial_value,
                at: Utc::now(),
            }),
            last_model_load_attempt: None,
            update_fn,
        }
    }

    fn set_last_model(&mut self, new_model: T) {
        self.last_model = Some(ModelValue {
            value: new_model,
            at: Utc::now(),
        });
    }

    pub fn update_model(&mut self, row: &R) -> Result<(), DataStoreError> {
        if let Some(model) = &self.last_model {
            let mut model_value = model.value.clone();
            (self.update_fn)(&mut model_value, row);
            self.last_model(model_value);

            self.db.put_json(LAST_MODEL_KEY, &self.last_model)?;
        }
        Ok(())
    }

    pub fn load_model(&mut self) {
        let lmv = self.db.read_json(LAST_MODEL_KEY);
        self.last_model = lmv;
        self.last_model_load_attempt = Some(Utc::now());
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> {
        self.last_model.as_ref().map(|m| m.at)
    }

    pub fn wipe_model(&mut self) -> Result<(), DataStoreError> {
        self.db.delete_all(LAST_MODEL_KEY)?;
        Ok(())
    }

    pub fn model(&self) -> Box<Option<ModelValue<T>>> {
        Box::new(self.last_model.clone())
    }

    pub fn value(&self) -> Option<T> {
        self.last_model.clone().map(|s| s.value)
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

    pub fn is_ready(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use db::Db;
    use tempfile::TempDir;

    use crate::model::BookPosition;
    use crate::ob_indicator_model::{IndicatorModel, ModelValue};
    use chrono::{DateTime, Utc};
    use quickcheck::{Arbitrary, Gen};
    use test::Bencher;
    use fake::Fake;

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

    fn test_db() -> Db {
        let tempdir = TempDir::new().unwrap();
        Db::new(tempdir.into_path().to_str().unwrap(), "temp".to_string())
    }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let mut table: IndicatorModel<MockLinearModel, TestRow> = IndicatorModel {
            db: test_db(),
            id: "default".to_string(),
            last_model: Some(ModelValue {
                value: MockLinearModel{},
                at: Utc::now(),
            }),
            last_model_load_attempt: None,
            update_fn: Box::new(|m, _r| m.clone()),
        };
        let mut gen = Gen::new(500);
        b.iter(|| table.update_model(&TestRow::arbitrary(&mut gen)).unwrap());
        table.load_model();
    }
}
