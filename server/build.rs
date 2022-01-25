use std::process::Command;

fn main() {
    let output = Command::new("git").args(&["rev-parse", "--short", "HEAD"]).output();
    let git_hash = match output {
        Ok(output) if output.status.success() => String::from_utf8(output.stdout).unwrap(),
        _ => {
            let msg = format!("{:?}", output);
            option_env!("BUILD_GIT_SHA").expect(&msg).to_string()
        }
    };
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
    //println!("cargo:rerun-if-changed=../../.git/HEAD");
    //println!("cargo:rerun-if-env-changed=BUILD_GIT_SHA");
}
