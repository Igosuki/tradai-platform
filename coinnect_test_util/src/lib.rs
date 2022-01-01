/*!
Test utils for `coinnect` isolated in a crate so as not to pollute the crate with feature toggles

 */

#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![allow(clippy::must_use_candidate)]

pub mod binance;
pub mod http;
