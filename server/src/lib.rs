#![deny(unused_must_use, unused_mut, unused_imports, unused_import_braces)]
#![feature(try_trait)]
#![feature(or_patterns)]

extern crate actix;
extern crate actix_derive;
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
extern crate log;
extern crate prometheus;
extern crate rand;
extern crate uuid;
#[macro_use]
extern crate juniper;
#[macro_use]
extern crate anyhow;

pub mod api;
pub mod graphql_schemas;
pub mod logging;
pub mod server;
pub mod settings;
pub mod system;
