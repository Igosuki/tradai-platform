#![feature(try_trait_v2)]

extern crate clap;
extern crate coinnect_rt;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
extern crate lazy_static;
extern crate log;
extern crate trader;

use std::io;

//lazy_static! {
//    static ref CONFIG_FILE: String = {
//        let trader_env : String = std::env::var("TRADER_ENV").unwrap_or("development".to_string());
//        format!("config/{}.yaml", trader_env)
//    };
//}
//
//lazy_static! {
//    static ref SETTINGS: RwLock<Config> = RwLock::new({
//        let mut settings = Config::default();
//        settings.merge(File::with_name(&CONFIG_FILE)).unwrap();
//
//        settings
//    });
//}
//
//fn show() {
//    println!(" * Settings :: \n\x1b[31m{:?}\x1b[0m",
//             SETTINGS
//                 .read()
//                 .unwrap()
//                 .clone()
//                 .try_into::<HashMap<String, String>>()
//                 .unwrap());
//}

#[actix_rt::main]
#[cfg_attr(feature = "flame_it", flame)]
async fn main() -> io::Result<()> { trader::runner::with_config(trader::system::start).await }
