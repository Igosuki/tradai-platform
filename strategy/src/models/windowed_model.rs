use std::ops::Index;
use std::slice::SliceIndex;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;

use db::Storage;
use ext::ResultExt;

use crate::error::Result;
use crate::models::persist::ModelValue;
use crate::models::{Model, TimedValue, TimedWindow, Window, WindowedModel};

use super::persist::{PersistentModel, PersistentVec};

type WindowFn<T, M> = for<'a> fn(&'a mut M, Window<'_, T>) -> &'a M;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct PersistentWindowedModel<T: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone> {
    rows: PersistentVec<T>,
    model: PersistentModel<M>,
    #[derivative(Debug = "ignore")]
    window_fn: WindowFn<T, M>,
}

impl<T: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone + Default>
    PersistentWindowedModel<T, M>
{
    pub fn new(
        id: &str,
        db: Arc<dyn Storage>,
        window_size: usize,
        max_size_o: Option<usize>,
        window_fn: WindowFn<T, M>,
        init: Option<M>,
    ) -> Self {
        let max_size = max_size_o.unwrap_or_else(|| (1.2 * window_size as f64) as usize);
        Self {
            rows: PersistentVec::new(db.clone(), &format!("{}_rows", id), max_size, window_size),
            model: PersistentModel::new(db, id, init.map(|i| ModelValue::new(i))),
            window_fn,
        }
    }

    pub fn update(&mut self) -> Result<()> {
        if self.is_filled() && !self.has_model() {
            self.model.set_last_model(M::default());
        }
        self.model.update(self.window_fn, self.rows.window()).err_into()
    }

    //pub fn model(&self) -> Option<ModelValue<M>> { self.model.model() }

    pub fn import(&mut self, model_value: serde_json::Value, table: serde_json::Value) -> Result<()> {
        let model: M = serde_json::from_value(model_value)?;
        self.wipe()?;
        self.model.set_last_model(model);
        let rows: Vec<TimedValue<T>> = serde_json::from_value(table)?;
        self.rows.push_all(rows)?;
        Ok(())
    }
}

impl<R, M> WindowedModel<R, M> for PersistentWindowedModel<R, M>
where
    M: Serialize + DeserializeOwned + Clone + Default,
    R: Serialize + DeserializeOwned + Clone,
{
    fn is_filled(&self) -> bool { self.rows.is_filled() }

    fn window(&self) -> Window<'_, R> { self.rows.window() }

    fn timed_window(&self) -> TimedWindow<'_, R> { self.rows.timed_window() }

    fn push(&mut self, row: &R) { self.rows.push(row); }

    fn len(&self) -> usize { self.rows.len() }

    fn is_empty(&self) -> bool { self.rows.is_empty() }
}

impl<R: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone + Default> Model<M>
    for PersistentWindowedModel<R, M>
{
    fn ser(&self) -> Option<serde_json::Value> { self.value().and_then(|m| serde_json::to_value(m).ok()) }

    fn try_load(&mut self) -> crate::error::Result<()> {
        self.model.try_loading()?;
        self.rows.try_loading()
    }

    fn is_loaded(&self) -> bool { self.model.is_loaded() && self.rows.is_loaded() }

    fn wipe(&mut self) -> Result<()> {
        self.model.wipe()?;
        self.rows.wipe()
    }

    fn last_model_time(&self) -> Option<DateTime<Utc>> { self.model.last_model_time() }

    fn has_model(&self) -> bool { self.model.has_model() }

    fn value(&self) -> Option<&M> { self.model.value() }
}

impl<
        R: Serialize + DeserializeOwned + Clone,
        M: Serialize + DeserializeOwned + Clone + Default,
        I: SliceIndex<[TimedValue<R>]>,
    > Index<I> for PersistentWindowedModel<R, M>
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output { &self.rows[index] }
}

#[cfg(test)]
mod test {
    extern crate test;

    use test::Bencher;

    use chrono::{DateTime, Utc};
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};

    use trading::book::BookPosition;

    use crate::models::{Model, Window};
    use crate::models::{PersistentWindowedModel, WindowedModel};
    use crate::test_util::test_db;

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

    fn sum_window<'a>(lm: &'a mut f64, window: Window<'_, TestRow>) -> &'a f64 {
        *lm = window.map(|t| t.pos.mid).sum::<f64>();
        lm
    }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let id = "default";
        let max_size = 2000;
        let db = test_db();
        let mut table = PersistentWindowedModel::new(id, db, 1000, Some(max_size), sum_window, None);
        let mut gen = Gen::new(500);
        for _ in 0..max_size {
            table.push(&TestRow::arbitrary(&mut gen))
        }
        b.iter(|| {
            table.update().unwrap();
            table.try_load().unwrap();
        });
    }
}
