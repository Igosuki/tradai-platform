use super::persist::{ModelValue, PersistentModel};
use chrono::{DateTime, Utc};
use db::DataStoreError;
use serde::de::DeserializeOwned;
use serde::Serialize;

type ModelUpdateFn<T, R> = fn(&T, &R) -> T;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct IndicatorModel<T, R> {
    model: PersistentModel<T>,
    #[derivative(Debug = "ignore")]
    update_fn: ModelUpdateFn<T, R>,
}

impl<T: Serialize + DeserializeOwned + Clone, R: Clone> IndicatorModel<T, R> {
    pub fn new(id: &str, db_path: &str, initial_value: T, update_fn: ModelUpdateFn<T, R>) -> Self {
        Self {
            model: PersistentModel::new(
                db_path,
                id.to_string(),
                Some(ModelValue {
                    value: initial_value,
                    at: Utc::now(),
                }),
            ),
            update_fn,
        }
    }

    pub fn update_model(&mut self, row: R) -> Result<(), DataStoreError> {
        let x = self.update_fn;
        self.model.update_model(x, &row)
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.model.last_model_time() }

    pub fn value(&self) -> Option<T> { self.model.value() }
}

#[cfg(test)]
mod test {}
