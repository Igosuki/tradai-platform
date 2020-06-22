#![feature(with_options)]
extern crate crc32fast;
extern crate rsgen_avro;
extern crate serde_json;

use crc32fast::Hasher;
use rsgen_avro::{Generator, Source};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::{env, io};

type AvroState = HashMap<String, u32>;

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-env-changed=AVRO_GEN");
    let _out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new("../target").join("avro");
    let gen_path_dir = Path::new("src").join("avro_gen");
    let gen_path = gen_path_dir.join("models.rs");

    let avro_state_file_p = Path::new("../target").join(".avro-processed.json");
    let mut avro_state_file = File::with_options()
        .create(true)
        .write(true)
        .truncate(false)
        .open(avro_state_file_p.clone())?;
    let avro_state_read = fs::read_to_string(avro_state_file_p);
    let option = avro_state_read
        .ok()
        .and_then(|read| serde_json::from_str(read.as_str()).ok());
    eprintln!("Original avro state : {:?}", option);
    let mut avro_state: AvroState = option.unwrap_or_else(|| serde_json::from_str("{}").unwrap());

    fs::create_dir_all(&dest_path).unwrap();
    fs::create_dir_all(&gen_path_dir).unwrap();

    let entries = fs::read_dir("./schemas")?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    let mut any_changes = false;
    for schema in entries {
        let file_name = schema.as_path();
        let mut hasher = Hasher::new();
        let schema_str = fs::read_to_string(file_name)?;
        hasher.update(schema_str.as_bytes());
        let checksum = hasher.finalize();
        let schema_file_name_str: Option<String> = file_name.to_str().map(|s| s.to_string());
        let original_checksum = schema_file_name_str
            .clone()
            .and_then(|f| avro_state.get(f.as_str()))
            .copied();
        if Some(checksum) == original_checksum {
            eprintln!(
                "Matching checksums {:?} {:?}",
                Some(checksum),
                original_checksum
            );
            continue;
        }
        eprintln!(
            "Non matching checksums {:?} {:?}",
            Some(checksum),
            original_checksum
        );
        schema_file_name_str.map(|s| avro_state.insert(s, checksum));
        any_changes = true;
        let output = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "java -jar ./tools/avro-tools-1.9.1.jar idl {} {}/{}.{}",
                file_name.display(),
                dest_path.as_path().display(),
                file_name.file_stem().unwrap().to_str().unwrap().to_string(),
                "avsc"
            ))
            .output();
        if let Err(e) = output {
            println!("Error generating avro schema : {:?}", e)
        }
    }
    if any_changes {
        let mut file = File::with_options()
            .write(true)
            .create(true)
            .open(gen_path.as_path())?;
        let x = dest_path.as_path();
        let source = Source::DirPath(x);
        let g = Generator::new().unwrap();
        g.gen(&source, &mut file).unwrap();
    }
    serde_json::to_string(&avro_state)
        .map_err(std::io::Error::from)
        .and_then(|st| avro_state_file.write_all(st.as_bytes()))
}
