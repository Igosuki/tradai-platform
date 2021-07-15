use db::Db;
use std::env;

const DUMP: &str = "dump";

fn main() {
    let args: Vec<String> = env::args().collect();
    let db = Db::new(&args[2], args[3].clone());
    let x: &str = &args[1];
    if x == DUMP {
        let strings = db.all();
        for string in strings {
            println!("{}", string);
        }
    }
}
