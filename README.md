### Configuration

Requires a `config/$TRADER_ENV.yaml` file in the cwd.

See test.yaml for a reference implementation.

### Building 

Default features 
```
make build
```

All features 
```
make build_all
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


