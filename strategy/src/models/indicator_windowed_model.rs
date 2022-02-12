use std::ops::Index;
use std::slice::SliceIndex;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;

use db::Storage;
use stats::Next;

use crate::error::Result;
use crate::models::persist::ModelValue;
use crate::models::{Model, TimedValue, TimedWindow, Window, WindowedModel};

use super::persist::{PersistentModel, PersistentVec};

// TODO: this repetead code is only here because PersistentVec returns an anonymous lifetime and we can't use Next<Window> with WindowFn
#[derive(Derivative)]
#[derivative(Debug)]
pub struct IndicatorWindowedModel<T: Serialize + DeserializeOwned, M: Serialize + DeserializeOwned> {
    rows: PersistentVec<T>,
    model: PersistentModel<M>,
}

impl<'a, T: 'a + Serialize + DeserializeOwned, M: 'a + Serialize + DeserializeOwned + Copy + Next<Window<'a, T>>>
    IndicatorWindowedModel<T, M>
{
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn new(id: &str, db: Arc<dyn Storage>, window_size: usize, max_size_o: Option<usize>, init: M) -> Self {
        let max_size = max_size_o.unwrap_or((1.2 * window_size as f64) as usize);
        Self {
            rows: PersistentVec::new(db.clone(), &format!("{}_rows", id), max_size, window_size),
            model: PersistentModel::new(db, id, Some(ModelValue::new(init))),
        }
    }

    pub fn update(&'a mut self) -> Result<()> {
        if let Some(model) = self.model.last_model.as_mut() {
            model.value.next(self.rows.window());
            self.model.persist()?;
        }
        Ok(())
    }

    pub fn import(&mut self, model_value: serde_json::Value, table: serde_json::Value) -> Result<()> {
        let model: M = serde_json::from_value(model_value)?;
        self.wipe()?;
        self.model.set_last_model(model);
        let rows: Vec<TimedValue<T>> = serde_json::from_value(table)?;
        self.rows.push_all(rows)?;
        Ok(())
    }
}

impl<R, M> WindowedModel<R, M> for IndicatorWindowedModel<R, M>
where
    M: Serialize + DeserializeOwned + Copy,
    R: Serialize + DeserializeOwned,
{
    fn is_filled(&self) -> bool { self.rows.is_filled() }

    fn window(&self) -> Window<'_, R> { self.rows.window() }

    fn timed_window(&self) -> TimedWindow<'_, R> { self.rows.timed_window() }

    fn push(&mut self, row: R) { self.rows.push(row); }

    fn len(&self) -> usize { self.rows.len() }

    fn is_empty(&self) -> bool { self.rows.is_empty() }
}

impl<'a, R: 'a + Serialize + DeserializeOwned, M: 'a + Serialize + DeserializeOwned + Copy> Model<M>
    for IndicatorWindowedModel<R, M>
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

    fn value(&self) -> Option<M> { self.model.value() }
}

impl<
        'a,
        R: Serialize + DeserializeOwned,
        M: Serialize + DeserializeOwned + Default,
        I: SliceIndex<[TimedValue<R>]>,
    > Index<I> for IndicatorWindowedModel<R, M>
{
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output { &self.rows[index] }
}
//
// #[cfg(test)]
// mod test {
//     extern crate test;
//
//     use test::Bencher;
//
//     use chrono::{DateTime, Utc};
//     use fake::Fake;
//     use quickcheck::{Arbitrary, Gen};
//     use stats::Next;
//
//     use trading::book::BookPosition;
//
//     use crate::models::indicator_windowed_model::IndicatorWindowedModel;
//     use crate::models::{Model, Window, WindowedModel};
//     use crate::test_util::test_db;
//
//     #[derive(Debug, Clone, Serialize, Deserialize)]
//     pub struct TestRow {
//         pub time: DateTime<Utc>,
//         pub pos: BookPosition, // crypto_1
//     }
//
//     impl Arbitrary for TestRow {
//         fn arbitrary(g: &mut Gen) -> TestRow {
//             let time: chrono::DateTime<Utc> = fake::faker::chrono::en::DateTime().fake();
//             TestRow {
//                 time,
//                 pos: BookPosition::arbitrary(g),
//             }
//         }
//     }
//
//     #[derive(Debug, Serialize, Deserialize, Default, Copy, Clone)]
//     struct TestModel {
//         sum: f64,
//     }
//
//     impl<'a> Next<Window<'a, TestRow>> for TestModel {
//         type Output = f64;
//
//         fn next(&mut self, window: Window<'a, TestRow>) -> Self::Output {
//             self.sum = window.map(|t| t.pos.mid).sum::<f64>();
//             self.sum
//         }
//     }
//
//     #[bench]
//     fn test_save_load_model(b: &mut Bencher) {
//         let id = "default";
//         let max_size = 2000;
//         let db = test_db();
//         let mut table = IndicatorWindowedModel::new(id, db, 1000, Some(max_size), TestModel::default());
//         let mut gen = Gen::new(500);
//         for _ in 0..max_size {
//             table.push(TestRow::arbitrary(&mut gen));
//             table.update().unwrap();
//         }
//         b.iter(|| {
//             table.update().unwrap();
//             table.try_load().unwrap();
//         });
//     }
// }
