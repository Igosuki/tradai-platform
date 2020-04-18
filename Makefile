## Cargo
CARGO_BIN ?= `which cargo`
TARGET_PATH ?= `pwd`/target/release
BIN_VERSION ?= 0.1.0
BIN_NAME ?= voik
BIN_PATH ?= $(TARGET_PATH)/$(BIN_NAME)

## Testing
FUNZZY_BIN ?= `which funzzy`

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
