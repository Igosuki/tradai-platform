use structopt::StructOpt;

use backtest::{Backtest, BacktestConfig};

#[derive(StructOpt, Debug)]
#[structopt(name = "backtest")]
struct BacktestCliOptions {
    #[structopt(short, long)]
    config: String,
}

#[actix::main]
async fn main() -> backtest::Result<()> {
    env_logger::init();
    let opts = BacktestCliOptions::from_args();
    let conf = BacktestConfig::new(opts.config)?;
    let bt = Backtest::try_new(&conf).await?;
    bt.run().await?;
    Ok(())
}
