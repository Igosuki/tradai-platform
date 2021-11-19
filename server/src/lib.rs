#![deny(unused_must_use, unused_mut, unused_imports, unused_import_braces)]
#![feature(try_trait_v2)]
#![feature(async_closure)]
#![feature(result_cloned)]
#![feature(map_try_insert)]
#![cfg_attr(feature = "flame_it", feature(proc_macro_hygiene))]

#[macro_use]
extern crate anyhow;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
#[macro_use]
extern crate juniper;
#[macro_use]
extern crate lazy_static;
extern crate measure_time;
#[macro_use]
extern crate tracing;

pub mod api;
mod connectivity;
pub mod graphql_schemas;
pub mod logging;
pub mod nats;
mod notify;
pub mod runner;
pub mod server;
pub mod settings;
pub mod system;
