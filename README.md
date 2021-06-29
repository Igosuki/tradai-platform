### Configuration

Requires a `config/$TRADER_ENV.yaml` file in the cwd.

See test.yaml for a reference implementation.

### Running infrastructure 

Install terraform 
```
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update && sudo apt-get install -y terraform
```

Run the plan

```
cd infra/prod/tf
terraform init
terraform plan -var-file=default.tfvars
```

Install ansible
```
sudo python3 -m pip install ansible  
```

Install ansible galaxy roles
```
infra/prod/install_roles.sh
```

Run the books to install software and config files on prod machines
```
infra/prod/run_books.sh
```

Deploy the trader (after running release found below)
```
infra/prod/deploy_trader.sh
```

### Building 

Default features 
```
make build
```

All features 
```
make build_all
```

### Releasing

If missing, build the rust-musl-builder-nightly docker image : 
```
git clone git@github.com:Igosuki/rust-musl-builder.git
cd rust-musl-builder
docker build -t rust-musl-builder-nightly --build-arg TOOLCHAIN=nightly .
```

Setup an alias

```
alias rust-musl-builder-nightly='docker run --cpus=$(nproc) --rm -it $MUSL_FLAGS -v "$(pwd)/cargo-git":/home/rust/.cargo/git -v "$(pwd)/cargo-registry":/home/rust/.cargo/registry -v "$(pwd)/cargo-target":/home/rust/src/target -v "$(pwd)":/home/rust/src rust-musl-builder-nightly'
```

Build the rust program

```
rust-musl-builder-nightly cargo build --release --target=x86_64-unknown-linux-musl 
```


### Tests and benchmarks

#### Requirements

The env var BITCOINS_TEST_RAMFS_DIR set to a ramfs disk, example : 

```sudo mount -t tmpfs -o size=2048M tmpfs /media/ramdisk```

#### Run
```
make test
make bench
```

#### Coverage
```
make coverage
```

### Deploying

Infrastructure files are found in `/infra`

### Profiling a process

Internally, for spans, there is the flamer crate.

Externally we can use flamegraph, gdb et. al.

```
cargo install flamegraph
cargo flamegraph --bin=trader -- [args]
``` 

#### Profiler links

- https://github.com/flamegraph-rs/flamegraph
- https://github.com/jonhoo/inferno
- https://github.com/bheisler/criterion.rs 

#### Debugging with rr 

Get the rr gdb configuration
```cp rr_gdbinit ~/.rr_gdbinit```

Follow the steps here https://github.com/rr-debugger/rr/wiki/Using-rr-in-an-IDE
and this specifically for rust https://gist.github.com/spacejam/15f27007c0b1bcc1d6b4c9169b18868c

#### Code coverage

- ```rustup component add llvm-tools-preview```
- ```export RUSTFLAGS="-Zinstrument-coverage"```

#### Linux specifics 

##### Dependencies

```
sudo apt-get install libfontconfig libfontconfig1-dev google-perftools libgoogle-perftools-dev
```

#### Administration

##### Database

For rocksdb, see https://github.com/facebook/rocksdb/wiki/Administration-and-Data-Access-Tool

Example : 
```
ldb --db=mean_reverting_BTC_USDT/ --column_family=models dump
```
This dumps the models table from the db.


