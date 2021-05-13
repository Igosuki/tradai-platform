## Cargo
CARGO_BIN ?= `which cargo`
TARGET_PATH ?= `pwd`/target/release
BIN_VERSION ?= 0.1.0
BIN_NAME ?= voik
BIN_PATH ?= $(TARGET_PATH)/$(BIN_NAME)

## Testing
FUNZZY_BIN ?= `which funzzy`

PWD ?= `pwd`

HOME ?= `echo $HOME`

.PHONY: build
build:
	@$(CARGO_BIN) build

.PHONY: build_all
build_all:
	@$(CARGO_BIN) build --all-features

.PHONY: test
test: ## Tests all features
	@$(CARGO_BIN) test --all-features

.PHONY: test_watcher ## Starts funzzy, test watcher, to run the tests on every change
test_watcher:
	@$(FUNZZY_BIN)

.PHONY: bench
bench:
	@$(CARGO_BIN) bench

.PHONY: profile
profile:
	@$(CARGO_BIN) flamegraph --dev --bin=trader --features flame_it

## alias rust-musl-builder-nightly='docker run --cpus=3 --rm -it --user rust $MUSL_FLAGS -v "$HOME/.cargo/git":/home/rust/.cargo/git -v "$(pwd)/cargo-registry":/home/rust/.cargo/registry -v "$(pwd)/cargo-target":/home/rust/src/target -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder:nightly-2020-04-17'
release:
	rust-musl-builder-nightly cargo build --release

build_test:
	@$(CARGO_BIN) test --message-format=json-diagnostic-rendered-ansi --color=always --no-run --lib $(TEST_NAME) --manifest-path $(MANIFEST_PATH)

flamegraph:
	perf script | inferno-collapse-perf > stacks.folded
	inferno-flamegraph stacks.folded > flamegraph.svg
	firefox flamegraph.svg
