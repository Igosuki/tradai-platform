#[macro_use]
extern crate tracing;

use structopt::StructOpt;

use backtest::{Backtest, BacktestConfig};

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
    #[cfg(feature = "console_tracing")]
    console_subscriber::init();
    env_logger::init();
    let opts = BacktestCliOptions::from_args();
    let conf = BacktestConfig::new(opts.config)?;
    match opts.cmd.unwrap_or(BacktestCmd::Run) {
        BacktestCmd::Run => {
            let mut bt = Backtest::try_new(&conf).await?;
            bt.run().await?;
            info!("Backtest finished.");
        }
        BacktestCmd::GenReport => {
            Backtest::gen_report(&conf).await;
        }
    }
    Ok(())
}
