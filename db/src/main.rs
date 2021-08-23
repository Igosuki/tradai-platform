use db::{get_or_create, DbEngineOptions, DbOptions, RocksDbOptions, StorageExt};
use rocksdb::{DBRecoveryMode, Options};
use std::path::PathBuf;
use structopt::clap::arg_enum;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "db_tool")]
struct CliOptions {
    #[structopt(short, long, default_value = "rocksdb")]
    db_type: DbType,
    #[structopt(short, long, parse(from_os_str))]
    path: PathBuf,
    #[structopt(subcommand)]
    cmd: DbCommand,
}

arg_enum! {
    #[derive(Debug)]
    enum DbType {
        Rocksdb,
    }
}
#[derive(StructOpt, Debug)]
#[structopt(about = "db command to execute")]
enum DbCommand {
    Dump {
        table: String,
    },
    Repair {
        #[structopt(long)]
        skip_corrupted: bool,
    },
}

fn main() {
    let options = CliOptions::from_args();
    let path: PathBuf = options.path;
    let db = get_or_create(
        &DbOptions {
            path: path.clone(),
            engine: match options.db_type {
                DbType::Rocksdb => DbEngineOptions::RocksDb(RocksDbOptions::default()),
            },
        },
        "",
        vec![],
    );
    match options.cmd {
        DbCommand::Dump { table } => {
            let all = db.get_all::<serde_json::Value>(&table).unwrap();
            for (_, v) in all {
                println!("{}", v);
            }
        }
        #[allow(clippy::single_match)]
        DbCommand::Repair { skip_corrupted } => match options.db_type {
            DbType::Rocksdb => {
                let mut options = Options::default();
                if skip_corrupted {
                    options.set_wal_recovery_mode(DBRecoveryMode::SkipAnyCorruptedRecord);
                }
                if let Err(e) = rocksdb::DB::repair(&options, path) {
                    eprintln!("Failed to repair db {}", e);
                } else {
                    println!("Repair successful");
                }
            }
        },
    }
}
