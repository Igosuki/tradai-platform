#[macro_use]
extern crate tracing;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate serde;

use backtest::Result;
use config::{Config, File};
use futures::FutureExt;
use structopt::StructOpt;
use typed_builder::TypedBuilder;

#[derive(StructOpt, Debug)]
#[structopt(name = "backtest")]
struct DataServerCliOptions {
    #[structopt(short, long)]
    config: String,
}

#[derive(Deserialize, TypedBuilder)]
pub struct DataServerConfig {
    pub api_port: usize,
}

impl DataServerConfig {
    /// # Panics
    ///
    /// if the config file does not exist
    pub fn new(config_file_name: String) -> Result<Self> {
        let mut s = Config::new();

        s.merge(File::with_name(&config_file_name)).unwrap();

        // You may also programmatically change settings
        s.set("__config_file", config_file_name)?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_into().map_err(Into::into)
    }
}

fn main() -> anyhow::Result<()> {
    actix::System::with_tokio_rt(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Default Tokio runtime could not be created.")
    })
    .block_on(run_main())
}

async fn run_main() -> anyhow::Result<()> {
    let opts = DataServerCliOptions::from_args();
    let _conf = DataServerConfig::new(opts.config)?;
    select! {
        _ = futures::future::ready(()) => {
            info!("Data server finished.");
        },
        _ = tokio::signal::ctrl_c().fuse() =>  {
            info!("Data server interrupted.");
            actix::System::current().stop();
        }
    }
    Ok(())
}
