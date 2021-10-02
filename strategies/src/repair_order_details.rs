use coinnect_rt::exchange::MockApi;
use db::DbOptions;
use std::path::PathBuf;
use std::sync::Arc;
use strategies::order_manager::OrderManager;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "db_tool")]
struct RepairOrderDetailsOptions {
    #[structopt(short, long, parse(from_os_str))]
    db_path: PathBuf,
}

#[tokio::main]
async fn main() {
    let options: RepairOrderDetailsOptions = RepairOrderDetailsOptions::from_args();
    let db_options = DbOptions::new(options.db_path);
    let manager = OrderManager::new(Arc::new(MockApi), &db_options, "");
    manager.repair_orders().await;
}
