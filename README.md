### Configuration

Requires a `config/$TRADER_ENV.yaml` file in the cwd.

See dev.yaml for a reference implementation.

### Running development infrastructure

```
cd infra/dev
docker-compose up -d
```

### Running production infrastructure

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

Individual builds for all targets are available as show in the makefile.

#### Multiple symbol issue

In case of multiple symbols for C library bindings, use the following rust flags.

```
# .cargo/config.toml
rustflags = ["-Clink-arg=-Wl,--allow-multiple-definition"]
```

### Releases

#### Binaries

Check out the makefile or cargo for the list of available binaries and necessary features

#### Linkers

In order to make releases building faster, you can use alternative linkers to llvm such as mold (linux, https://github.com/rui314/mold) or zld (mac, https://github.com/michaeleisel/zld) or lld.

Check out the config.toml.sample file for rustc flags

#### Releasing builds with docker

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
rust-musl-builder-nightly cargo build --release --target=x86_64-unknown-linux-gnu 
```

### Tests and benchmarks

#### Requirements

The environment variable BITCOINS_TEST_RAMFS_DIR must be set to a ram disk if you want to speed up integration tests. See the macramdisk and linuxramdisk makefile targets for reference.

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

And not forgetting proper rust flags, for instance : 
```~/.cargo/config.toml
[target.x86_64-unknown-linux-gnu]
#linker="clang"
rustflags = [
    "-Clink-arg=-fuse-ld=lld", "-Clink-arg=-Wl,--no-rosegment", "--cfg", "tokio_unstable", "-g"
]
```

#### Profiler links

- https://github.com/flamegraph-rs/flamegraph
- https://github.com/jonhoo/inferno
- https://github.com/bheisler/criterion.rs

#### Debugging with rr

Install

```
cd /tmp
wget https://github.com/rr-debugger/rr/releases/download/5.4.0/rr-5.4.0-Linux-$(uname -m).deb
sudo dpkg -i rr-5.4.0-Linux-$(uname -m).deb
```

Get the rr gdb configuration
```cp rr_gdbinit ~/.rr_gdbinit```

Follow the steps here https://github.com/rr-debugger/rr/wiki/Using-rr-in-an-IDE
and this specifically for rust https://gist.github.com/spacejam/15f27007c0b1bcc1d6b4c9169b18868c

#### Profilign with cargo 

See https://github.com/svenstaro/cargo-profiler

#### Profiling heap allocations

```RUST_LOG=debug heaptrack ./target/release/trader -c $config```

#### Profiling with valgrind

```RUST_LOG=debug valgrind --tool=massif ./target/debug/trader -c $config```

#### Profiling with cachegrind 

```RUST_LOG=debug valgrind --tool=cachegrind ./target/debug/trader -c $config```

#### Profiling with oprofile

```RUST_LOG=debug operf ./target/debug/trader -c $config```

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


