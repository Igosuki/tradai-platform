// #[derive(StructOpt, Debug)]
// #[structopt(name = "basic")]
// #[clap(version = "1.0", author = "Guillaume B. <igosuki.github@gmail.com>")]
// struct CliOptions {
//     #[structopt(long)]
//     use_test_servers: bool,
//     #[structopt(short, long)]
//     exchange: Exchange,
//     #[structopt(short, long)]
//     keys_file: PathBuf,
//     #[clap(subcommand)]
//     cmd: Command,
// }
//
// enum Command {
//     DownloadPairs(DownloadPairs),
// }
//
// #[derive(Clap)]
// struct DownloadPairs {
//     /// Outfile
//     #[clap(short)]
//     out: PathBuf,
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let opts: CliOptions = CliOptions::from_args();
    // let _api = Broker::build_exchange_api(opts.keys_file, &opts.exchange, opts.use_test_servers).await?;
    Ok(())
}
