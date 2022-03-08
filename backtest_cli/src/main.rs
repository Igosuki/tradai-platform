#[macro_use]
extern crate tracing;
#[macro_use]
extern crate futures;

use backtest::{Backtest, BacktestConfig};
use futures::FutureExt;
use structopt::StructOpt;
#[cfg(feature = "python")]
#[allow(unused_imports)]
use tradai_python::script_strat;

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
    actix::System::with_tokio_rt(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Default Tokio runtime could not be created.")
    })
    .block_on(run_main())
}

async fn run_main() -> anyhow::Result<()> {
    strategies::init();
    #[cfg(feature = "python")]
    tradai_python::prepare();
    #[cfg(feature = "console_tracing")]
    util::trace::init_console_subscriber();
    #[cfg(not(feature = "console_tracing"))]
    util::trace::init_tracing_env_subscriber();

    let opts = BacktestCliOptions::from_args();
    let conf = BacktestConfig::new(opts.config)?;
    match opts.cmd.unwrap_or(BacktestCmd::Run) {
        BacktestCmd::Run => {
            let mut bt = Backtest::try_new(&conf).await?;
            select! {
                r = bt.run().fuse() => {
                    r?;
                    info!("Backtest finished.");
                },
                _ = tokio::signal::ctrl_c().fuse() =>  {
                    info!("Backtest interrupted.");
                    actix::System::current().stop();
                }
            }
        }
        BacktestCmd::GenReport => {
            Backtest::gen_report(&conf).await;
        }
    }
    Ok(())
}
