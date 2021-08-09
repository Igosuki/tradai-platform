#![deny(unused_must_use, unused_mut, unused_imports, unused_import_braces)]
#![feature(try_trait_v2)]
#![feature(async_closure)]
#![feature(result_cloned)]
#![feature(map_try_insert)]
#![cfg_attr(feature = "flame_it", feature(proc_macro_hygiene))]

extern crate actix;
extern crate actix_derive;
#[macro_use]
extern crate anyhow;
extern crate byte_unit;
extern crate clap;
extern crate coinnect_rt;
extern crate config;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
#[macro_use]
extern crate juniper;
#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate tracing;
extern crate prometheus;
extern crate rand;
extern crate uuid;

pub mod api;
pub mod graphql_schemas;
pub mod logging;
pub mod nats;
mod notify;
pub mod runner;
pub mod server;
pub mod settings;
pub mod system;
