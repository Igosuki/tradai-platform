pub mod indicator_model;
mod persist;
pub mod windowed_model;

pub use indicator_model::IndicatorModel;
pub use persist::Window;
use serde::Serialize;
pub use windowed_model::WindowedModel;

pub trait Model<M: Serialize> {
    fn value(&self) -> Option<M>;
}
