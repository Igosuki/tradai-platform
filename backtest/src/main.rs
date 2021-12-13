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

#[actix::main]
async fn main() -> backtest::Result<()> {
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
