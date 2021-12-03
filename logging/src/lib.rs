#![allow(clippy::derivable_impls)]

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
