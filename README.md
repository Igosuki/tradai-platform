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

#### Linux specifics 

##### Dependencies

```
sudo apt-get install libfontconfig libfontconfig1-dev google-perftools libgoogle-perftools-dev
```
