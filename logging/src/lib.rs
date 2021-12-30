#![allow(
    clippy::derivable_impls,
    clippy::similar_names,
    clippy::module_name_repetitions,
    clippy::must_use_candidate
)]

#[macro_use]
extern crate anyhow;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tracing;

pub mod prelude {
    pub use crate::file::file_actor::{AvroFileActor, FileActorOptions};
    pub use crate::file::{Partition, Partitioner};
    pub use crate::market_event::MarketEventPartitioner;
}

mod avro_gen;
mod file;
mod market_event;
