#[macro_use]
extern crate tracing;
#[macro_use]
extern crate futures;

use backtest::{Backtest, BacktestConfig};
use futures::FutureExt;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
enum BacktestCmd {
    Run,
    GenReport,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "backtest")]
struct BacktestCliOptions {
    #[structopt(short, long)]
    config: String,
    #[structopt(subcommand)]
    cmd: Option<BacktestCmd>,
}

fn main() -> anyhow::Result<()> {
    strategy_python::prepare();
    let mut builder = pyo3_asyncio::tokio::re_exports::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_asyncio::tokio::init(builder);
    actix::System::with_tokio_rt(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Default Tokio runtime could not be created.")
    })
    .block_on(run_main())
}

async fn run_main() -> anyhow::Result<()> {
    #[cfg(feature = "console_tracing")]
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        info!("Initializing tokio console subscriber");
        let console_layer = console_subscriber::spawn();
        let env_filter = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::registry()
            .with(console_layer)
            .with(tracing_subscriber::fmt::layer())
            .with(env_filter)
            .init();
    }
    #[cfg(not(feature = "console_tracing"))]
    {
        init_tracing_env_subscriber();
    }
    let opts = BacktestCliOptions::from_args();
    let conf = BacktestConfig::new(opts.config)?;
    match opts.cmd.unwrap_or(BacktestCmd::Run) {
        BacktestCmd::Run => {
            let mut bt = Backtest::try_new(&conf).await?;
            'run: loop {
                select! {
                    r = bt.run().fuse() => {
                        r?;
                        info!("Backtest finished.");
                    },
                    _ = tokio::signal::ctrl_c().fuse() =>  {
                        info!("Backtest interrupted.");
                        actix::System::current().stop();
                        break 'run;
                    }
                }
            }
        }
        BacktestCmd::GenReport => {
            Backtest::gen_report(&conf).await;
        }
    }
    Ok(())
}
