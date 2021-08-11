use super::persist::{ModelValue, PersistentModel};
use chrono::{DateTime, Utc};
use db::Storage;
use math::Next;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

type ModelUpdateFn<T, R> = fn(&T, R) -> T;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct IndicatorModel<T, R> {
    model: PersistentModel<T>,
    #[derivative(Debug = "ignore")]
    update_fn: ModelUpdateFn<T, R>,
}

impl<T: Serialize + DeserializeOwned + Clone + Next<R>, R: Clone> IndicatorModel<T, R> {
    pub fn new(id: &str, db: Arc<dyn Storage>, initial_value: T) -> Self {
        Self {
            model: PersistentModel::new(
                db,
                id,
                Some(ModelValue {
                    value: initial_value,
                    at: Utc::now(),
                }),
            ),
            update_fn: |m, args| {
                let mut new_m = m.clone();
                new_m.next(args);
                new_m
            },
        }
    }

    pub fn update_model(&mut self, next_value: R) -> Result<(), db::Error> {
        self.model.update_model(self.update_fn, next_value)
    }

    #[allow(dead_code)]
    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.model.last_model_time() }

    pub fn value(&self) -> Option<T> { self.model.value() }

    pub fn try_loading_model(&mut self) -> crate::error::Result<()> { self.model.try_loading() }

    pub fn is_loaded(&self) -> bool { self.model.is_loaded() }
}

#[cfg(test)]
mod test {}
