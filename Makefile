VENV := precommit_venv
HOOKS := .git/hooks/pre-commit

# PRE-COMMIT HOOKS
$(VENV): .requirements-precommit.txt
	virtualenv -p python3 $(VENV)
	$(VENV)/bin/pip install -r .requirements-precommit.txt
	pre-commit install --hook-type commit-msg
	pre-commit install --hook-type prepare-commit-msg

.PHONY: env
env: $(VENV)

.PHONY: clean-env
clean-env:
	rm -rf $(VENV)

## Cargo
CARGO_BIN ?= `which cargo`
TARGET_PATH ?= `pwd`/target/release
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
	@$(CARGO_BIN) diesel_cli --help > /dev/null || cargo install diesel_cli --no-default-features --features postgres
	@$(CARGO_BIN) sqlx --help > /dev/null || cargo install sqlx-cli --no-default-features --features sqlite


mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(notdir $(patsubst %/,%,$(dir $(mkfile_path))))

### CI

.PHONY: install-cargo-tools
install-cargo-tools:
	@$(CARGO_BIN) install flamegraph cargo-llvm-lines cargo-bloat cargo-edit cargo-deps cargo-bump cargo-clean-recursive cargo-criterion cargo-expand cargo-profiler cargo-tarpaulin cargo-udeps
	@$(CARGO_BIN) install --git https://github.com/Igosuki/cargo-outdated

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

.PHONY: maxopenfilesmac
maxopenfilesmac:
	ulimit -Sn 65536 200000

.PHONY: maxopenfileslinux
maxopenfileslinux:
	ulimit -Sn 350000

.PHONY: test-all
test-all: ## Tests all features
	@$(CARGO_BIN) test --all-features

.PHONY: test
test: ## Tests all features and targets, skipping coinnect
	RUST_LOG=info BITCOINS_REPO=$(current_dir)/.. $(CARGO_BIN) test --all-targets -- --skip coinnect_tests --skip coinbase_tests

.PHONY: test-strats
test-strats: ## Tests strategies
	RUST_LOG=info BITCOINS_REPO=$(current_dir)/.. $(CARGO_BIN) test --package strategies

.PHONY: coverage
coverage: ## Tests all features
	@$(CARGO_BIN) tarpaulin -v --avoid-cfg-tarpaulin -o Html --skip-clean --ignore-tests

.PHONY: test-watcher ## Starts funzzy, test watcher, to run the tests on every change
test-watcher:
	@$(FUNZZY_BIN)

.PHONY: bench
bench:
	@$(CARGO_BIN) bench

### PROFILING

.PHONY: rustc-self-profile
rustc-self-profile:
	RUSTC_BOOTSTRAP=1 cargo rustc -- -Zself-profile -Z self-profile-events=default,args

.PHONY: summarize-prof-data
summarize-prof-data:
	summarize summarize $(prof_data) | head -10

.PHONY: summarize-diff-prof-data
summarize-diff-prof-data:
	summarize diff $(prof_data) | head -10

.PHONY: chrome-prof-data-convert
chrome-prof-data-convert:
	crox --minimum-duration 500000 $(prof_data)

.PHONY: profile
profile:
	@$(CARGO_BIN) flamegraph --dev --bin=trader --features flame_it

.PHONY: flamegraph_stack
flamegraph_stack:
	perf script | inferno-collapse-perf > stacks.folded
	inferno-flamegraph stacks.folded > flamegraph.svg
	firefox flamegraph.svg

.PHONE: flamegraph
flamegraph:
	LD_LIBRARY_PATH=/usr/local/lib CARGO_PROFILE_RELEASE_DEBUG=true RUST_BACKTRACE=1 RUST_LOG=info CARGO_HOME=.cargo_debug $(CARGO_BIN) flamegraph $(extras) --no-inline --bin $(target) --features=$(features) -- $(args)

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
features=release_default
profile=release
target_arch=x86_64-unknown-linux-gnu
.PHONY: release
release:
	mkdir -p build/binaries
	docker run --cpus=$(shell nproc) --rm -it -v "$(PWD)/build/cargo-git":/home/rust/.cargo/git:rw -v "$(PWD)/build/cargo-registry":/home/rust/.cargo/registry -v "$(PWD)/build/cargo-target":/home/rust/src/target -v "$(PWD)":/home/rust/src -v "$(PWD)/config_release.toml":/home/rust/src/.cargo/config.toml -e LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu -e BUILD_GIT_SHA="$(GIT_SHA)" -e CFLAGS=-I/usr/local/musl/include -e CC=musl-gcc rust-musl-builder-nightly cargo build --bin $(target) --profile $(profile) --target=$(target_arch) --no-default-features --features=$(features) -Z unstable-options
	cp build/cargo-target/$(target_arch)/release/$(target) build/binaries/$(target)

release-local-debug:
	LD_LIBRARY_PATH=/usr/local/lib CARGO_PROFILE_RELEASE_DEBUG=true CARGO_HOME=.cargo_debug $(CARGO_BIN) build --release --bin $(target) --features=$(features)

release-trader-musl:
	make target_arch=x86_64-unknown-linux-musl release

release-trader:
	make target=trader release

release-debug-trader:
	make profile=release-debug release

release-db-tool:
	make target=db_tool release

release-om-tool:
	make target=om_tool release

release-backtest:
	make target=backtest

release-local-backtest:
	@$(CARGO_BIN) build --release --bin backtest --features=release_default

release-local-backtest-debug:
	make features=release_default target=backtest release_local_debug

release-local-backtest-ballista:
	@$(CARGO_BIN) build --release --bin backtest --features=release_default,remote_execution

bin_tag=latest
download-binary:
	mkdir -p build/binaries
	aws --profile btcfeed s3 cp --endpoint=https://nyc3.digitaloceanspaces.com s3://btcfeed/binaries/$(target)/$(target)-$(bin_tag) build/binaries/$(target)

### Python library

python_target=python3.10
python_arch=linux_x86_64
python_cp_target=cp310
python_dylib_v=2021.0.0
release-python-lib-local:
	maturin build --release --no-sdist --rustc-extra-args="-Clink-arg=-Wl,--allow-multiple-definition" -i $(python_target) -m python_dylib/Cargo.toml && pip3.10 install --force-reinstall $(PWD)/python_dylib/target/wheels/tradai-$(python_dylib_v)-$(python_cp_target)-$(python_cp_target)-$(python_arch).whl

build-python-lib-local:
	maturin build --no-sdist -i $(python_target) -m python_dylib/Cargo.toml --rustc-extra-args="-Clink-arg=-Wl,--allow-multiple-definition" && pip3.10 install --force-reinstall $(PWD)/python_dylib/target/wheels/tradai-$(python_dylib_v)-$(python_cp_target)-$(python_cp_target)-$(python_arch).whl

dev-python-lib:
	maturin develop -m python_dylib/Cargo.toml --extras pytest,pytest-cov[all] --rustc-extra-args="-Clink-arg=-Wl,--allow-multiple-definition"
	echo "Build finished, check ./target/wheels"

mx_cargo_home=/root/.cargo
release-python-lib-docker:
	docker run --rm --cpus=$(shell nproc) -it -e LD_LIBRARY_PATH=/opt/python/python3.10/lib \
	-e BUILD_GIT_SHA="$(GIT_SHA)" -v "$(PWD)/build/cargo-git":$(mx_cargo_home)/git:rw -v "$(PWD)/build/cargo-registry":$(mx_cargo_home)/registry \
	-v "$(PWD)/build/cargo-target":/io/target_release -v "$(PWD)":/io -v "$(PWD)/config_release.toml":/io/.cargo/config.toml maturin_builder \
	build --release --no-sdist -i $(python_target) -m python_dylib/Cargo.toml
	echo "Build finished, check ./target/wheels"

maturin-builder-image:
	docker build -f python_dylib/Dockerfile -t maturin_builder python_dylib

### DOCKER

docker-up:
	docker-compose -f infra/dev/docker-compose.yaml up -d

docker-down:
	docker-compose -f infra/dev/docker-compose.yaml down

docker-logs:
	docker-compose -f infra/dev/docker-compose.yaml logs -f $(SERVICE)

### Deploy

deploy-trader:
	./infra/prod/deploy_trader.sh

deploy-feeder24:
	./infra/prod/deploy_feeder24.sh

deploy-tools:
	./infra/prod/deploy_tools.sh

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

.PHONY: check-unused-deps
check-unused-deps:
	cargo +nightly udeps

.PHONY: check-dep-graph
check-deps-graph:
	cargo deps --filter $(cargo metadata --format-version 1 | jq '.workspace_members[]' -r | cut -d ' ' -f 1 | tr '\n' ' ') | dot -Tsvg > depgraph.svg
	echo "Wrote dependency graph to depgraph.svg"

### Database Management

migrate-engine-db:
	 diesel migration run --migration-dir strategies/migrations
