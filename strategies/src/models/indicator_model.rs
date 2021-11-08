use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;

use db::Storage;
use ext::ResultExt;
use math::Next;

use crate::error::Result;
use crate::models::Model;

use super::persist::{ModelValue, PersistentModel};

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
            model: PersistentModel::new(db, id, Some(ModelValue::new(initial_value))),
            update_fn: |m, args| {
                let mut new_m = m.clone();
                new_m.next(args);
                new_m
            },
        }
    }

    pub fn update(&mut self, next_value: R) -> Result<()> { self.model.update(self.update_fn, next_value).err_into() }

    #[allow(dead_code)]
    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.model.last_model_time() }

    pub fn value(&self) -> Option<T> { self.model.value() }

    pub fn is_loaded(&self) -> bool { self.model.is_loaded() }

    pub fn wipe(&mut self) -> Result<()> { self.model.wipe().err_into() }

    pub fn import(&mut self, v: serde_json::Value) -> Result<()> {
        let model: T = serde_json::from_value(v)?;
        self.model.wipe()?;
        self.model.set_last_model(model);
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned + Clone + Next<R>, R: Clone> Model for IndicatorModel<T, R> {
    fn ser(&self) -> Option<serde_json::Value> { self.value().and_then(|m| serde_json::to_value(m).ok()) }

    fn try_load(&mut self) -> crate::error::Result<()> { self.model.try_loading() }
}

#[cfg(test)]
mod test {}
