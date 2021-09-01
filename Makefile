VENV := precommit_venv
HOOKS := .git/hooks/pre-commit

# PRE-COMMIT HOOKS
$(VENV): .requirements-precommit.txt
	virtualenv -p python3 $(VENV)
	$(VENV)/bin/pip install -r .requirements-precommit.txt

.PHONY: env
env: $(VENV)

.PHONY: clean-env
clean-env:
	rm -rf $(VENV)

## Cargo
CARGO_BIN ?= `which cargo`
TARGET_PATH ?= `pwd`/target/release
BIN_VERSION ?= 0.1.0
BIN_NAME ?= voik
BIN_PATH ?= $(TARGET_PATH)/$(BIN_NAME)
GIT_SHA ?= `git rev-parse --short HEAD`

## Testing
FUNZZY_BIN ?= `which funzzy`

PWD ?= `pwd`

HOME ?= `echo $HOME`

$(HOOKS): $(VENV) .pre-commit-config.yaml
	$(VENV)/bin/pre-commit install -f --install-hooks
	@$(CARGO_BIN) fmt --help > /dev/null || rustup component add rustfmt
	@$(CARGO_BIN) clippy --help > /dev/null || rustup component add clippy
	@$(CARGO_BIN) readme --help > /dev/null || cargo install cargo-readme

mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))

### CI

.PHONY: install-cargo-tools
install-cargo-tools: @$(CARGO_BIN) install cargo-flamegraph cargo-llvm-lines cargo-bloat cargo-edit

.PHONY: install-hooks
install-hooks: $(HOOKS)

.PHONY: clean-hooks
clean-hooks:
	rm -rf $(HOOKS)

.PHONY: all
all: release

### BUILD

.PHONY: clean
clean:
	@$(CARGO_BIN) clean

.PHONY: build
build:
	@$(CARGO_BIN) build

.PHONY: compile
compile:
	@$(CARGO_BIN) build --all-features --all-targets

.PHONY: build_all
build_all:
	@$(CARGO_BIN) build --all-features

.PHONY: build_test
build_test:
	@$(CARGO_BIN) test --message-format=json-diagnostic-rendered-ansi --color=always --no-run --lib $(TEST_NAME) --manifest-path $(MANIFEST_PATH)

### TESTS

.PHONY: macramdisk
macramdisk:
	diskutil erasevolume HFS+ 'RAM Disk' `hdiutil attach -nobrowse -nomount ram://262144`

.PHONY: linuxramdisk
linuxramdisk:
	sudo mount -t tmpfs -o size=2048M tmpfs /media/ramdisk

.PHONY: test_all
test_all: ## Tests all features
	@$(CARGO_BIN) test --all-features

.PHONY: test
test: ## Tests all features and targets, skipping coinnect
	RUST_LOG=info BITCOINS_REPO=$(current_dir)/.. $(CARGO_BIN) test --all-targets -- --skip coinnect_tests --skip gdax_tests

.PHONY: test_strats
test_strats: ## Tests strategies
	RUST_LOG=info BITCOINS_REPO=$(current_dir)/.. $(CARGO_BIN) test --package strategies

.PHONY: coverage
coverage: ## Tests all features
	@$(CARGO_BIN) tarpaulin -v --avoid-cfg-tarpaulin -o Html --skip-clean --release

.PHONY: test_watcher ## Starts funzzy, test watcher, to run the tests on every change
test_watcher:
	@$(FUNZZY_BIN)

.PHONY: bench
bench:
	@$(CARGO_BIN) bench

### PROFILING

.PHONY: profile
profile:
	@$(CARGO_BIN) flamegraph --dev --bin=trader --features flame_it

.PHONY: flamegraph
flamegraph:
	perf script | inferno-collapse-perf > stacks.folded
	inferno-flamegraph stacks.folded > flamegraph.svg
	firefox flamegraph.svg

.PHONY: filt
filt:
	rustfilt -i perf.data -o perf_de.data

### LINT
.PHONY: lint
lint:
	@$(CARGO_BIN) clippy --all-targets --all-features -Z unstable-options -- -Dclippy::all -Dunused_imports

.PHONY: lintfix
lintfix:
	@$(CARGO_BIN) clippy --fix --all-targets --all-features -Z unstable-options -- -Dclippy::all -Dunused_imports

.PHONY: clean-lint
clean-lint:
	find . -type f -name *.rs.bk -delete

### RELEASE

## alias rust-musl-builder-nightly='docker run --cpus=$(nproc) --rm -it --user rust $MUSL_FLAGS -v "$HOME/.cargo/git":/home/rust/.cargo/git -v "$(pwd)/cargo-registry":/home/rust/.cargo/registry -v "$(pwd)/cargo-target":/home/rust/src/target -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder:nightly-2020-06-17'
#$(rust-musl-builder-nightly) cargo build --release --target=x86_64-unknown-linux-gnu
target=trader
features=rocksdb-vendor,zstd
profile=release
.PHONY: release
release:
	docker run --cpus=$(shell nproc) --rm -it -v "$(PWD)/cargo-git":/home/rust/.cargo/git -v "$(PWD)/cargo-registry":/home/rust/.cargo/registry -v "$(PWD)/cargo-target":/home/rust/src/target -v "$(PWD)":/home/rust/src -v "$(PWD)/config_release.toml":/home/rust/src/.cargo/config.toml -e LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu -e BUILD_GIT_SHA="$(GIT_SHA)" -e CFLAGS=-I/usr/local/musl/include -e CC=musl-gcc rust-musl-builder-nightly cargo build --bin $(target) --profile $(profile) --target=x86_64-unknown-linux-gnu --no-default-features --features=$(features) -Z unstable-options

release_trader: release

release_debug_trader:
	make profile=release-debug release

release_db_tool:
	make target=db_tool features=rocksdb-vendor,zstd,structopt release

release_backtest:
	make target=backtest release

python_target=python3.9
build_python_lib:
	maturin build -i $(python_target) --cargo-extra-args="--features=static"

### DOCKER

docker-up:
	docker-compose -f infra/dev/docker-compose.yaml up -d

docker-down:
	docker-compose -f infra/dev/docker-compose.yaml down

docker-logs:
	docker-compose -f infra/dev/docker-compose.yaml logs -f $(SERVICE)

### Deploy

deploy_trader:
	./infra/prod/deploy_trader.sh

deploy_feeder24:
	./infra/prod/deploy_feeder24.sh

deploy_feeder:
	./infra/prod/deploy_feeder.sh

### Checks

.PHONY: check-bloat
check-bloat:
	strings target/release/trader > strings
	cat strings | awk '{ print length, $$0 }' | sort -n -s | cut -d" " -f2- > exec_strings_sorted.txt
	rm strings
	echo "Wrote sorted strings in exec_strings_sorted.txt"

.PHONY: check-deps
check-deps:
	readelf -d target/release/trader | grep 'NEEDED'
