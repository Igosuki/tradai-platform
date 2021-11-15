use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;
use strum_macros::EnumString;

use coinnect_rt::exchange::MockExchangeApi;
use db::DbOptions;
use strategies::order_manager::types::TransactionStatus;
use strategies::order_manager::OrderManager;

#[derive(StructOpt, Debug, EnumString)]
enum Cmd {
    #[strum(serialize = "repair_orders")]
    RepairOrders,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "om_tool")]
struct RepairOrderDetailsOptions {
    #[structopt(short, long, parse(from_os_str))]
    db_path: PathBuf,
    #[structopt(short, long)]
    cmd: Cmd,
}

#[tokio::main]
async fn main() {
    let options: RepairOrderDetailsOptions = RepairOrderDetailsOptions::from_args();
    let db_options = DbOptions::new(options.db_path);
    let manager = OrderManager::new(Arc::new(MockExchangeApi::default()), &db_options, "");
    match options.cmd {
        Cmd::RepairOrders => {
            manager.repair_orders().await;
        }
    }
}
