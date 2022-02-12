use std::path::PathBuf;
use std::sync::Arc;

use structopt::StructOpt;
use strum_macros::EnumString;

use brokers::api::{Brokerage, MockBrokerage};
use brokers::manager::{BrokerageManager, BrokerageManagerRef, BrokerageRegistry};
use db::{get_or_create, DbOptions};
use trading::order_manager::OrderManager;

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
    let mock_api: Arc<dyn Brokerage> = Arc::new(MockBrokerage::default());
    let apis = BrokerageRegistry::new();
    apis.insert(mock_api.exchange(), mock_api);
    let exchange_manager = BrokerageManagerRef::new(BrokerageManager::new_with_reg(apis));
    let db = get_or_create(&db_options, "", vec![]);
    let manager = OrderManager::new(exchange_manager, db);
    match options.cmd {
        Cmd::RepairOrders => {
            manager.repair_orders().await;
        }
    }
}
