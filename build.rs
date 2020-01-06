#![feature(with_options)]
extern crate rsgen_avro;

use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::{env, io};
use rsgen_avro::{Source, Generator};

fn main() -> io::Result<()> {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new("target").join("avro");
    let gen_path_dir = Path::new("src").join("avro_gen");
    let gen_path = gen_path_dir.join("models.rs");
    fs::create_dir_all(&dest_path).unwrap();
    fs::create_dir_all(&gen_path_dir).unwrap();
    let mut entries = fs::read_dir("./schemas")?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    for schema in entries {
        let file_name = schema.as_path();
        let output = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "java -jar ./tools/avro-tools-1.9.1.jar idl {} {}/{}.{}",
                file_name.display(),
                dest_path.as_path().display(),
                format!("{}", file_name.file_stem().unwrap().to_str().unwrap()),
                "avsc"
            ))
            .output();
        match output {
            Err(e) => println!("Error generating avro schema : {:?}", e),
            _ => ()
        }

    }
    let mut file = File::with_options().write(true).create(true).open(gen_path.as_path())?;
    let x = dest_path.as_path();
    let source = Source::DirPath(x);
    let g = Generator::new().unwrap();
    g.gen(&source, &mut file).unwrap();
    Ok(())
}
