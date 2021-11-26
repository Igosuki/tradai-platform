use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::{DBRecoveryMode, Options};
use structopt::clap::arg_enum;
use structopt::StructOpt;

use db::{get_or_create, DbEngineOptions, DbOptions, RocksDbOptions, Storage, StorageExt};

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
        #[structopt(long)]
        readonly: bool,
    },
    Repair {
        #[structopt(long)]
        skip_corrupted: bool,
    },
    Compact {
        table: Option<String>,
        #[structopt(long)]
        from: Option<String>,
        #[structopt(long)]
        to: Option<String>,
    },
}

fn main() {
    let options = CliOptions::from_args();
    let path: PathBuf = options.path.clone();
    match options.cmd {
        DbCommand::Dump { ref table, readonly } => {
            let db = db(options.db_type, &path, readonly, vec![table.clone()]);
            let all = db.get_all::<serde_json::Value>(table).unwrap();
            for (k, v) in all {
                println!("{{\"id\": \"{:?}\", \"transaction\": {}}}", k, v);
            }
        }
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
        DbCommand::Compact { from, to, table } => {
            if let Some(t) = table {
                let db = rocksdb::DB::open_cf(&Options::default(), path, vec![t.clone()]).unwrap();
                let handle = db.cf_handle(&t).unwrap();
                db.compact_range_cf(&handle, from, to)
            } else {
                let db = rocksdb::DB::open(&Options::default(), path).unwrap();
                db.compact_range(from, to)
            };
            println!("Compaction ended");
        }
    }
}

fn db(db_type: DbType, path: &Path, read_only: bool, tables: Vec<String>) -> Arc<dyn Storage> {
    get_or_create(
        &DbOptions {
            path,
            engine: match db_type {
                DbType::Rocksdb => DbEngineOptions::RocksDb(RocksDbOptions::default().read_only(read_only)),
            },
        },
        "",
        tables,
    )
}
