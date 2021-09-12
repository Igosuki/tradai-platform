### Introduction

This is a fully fledged trading platform, allowing you to run stateful and persisted trading strategies, interact with
cryptocurrency exchange platforms, gather and query data, and run backtests.

#### Binaries

##### Trader

This program is a distributed actor based trading system that can be launched altogether or in separate processes.
See `trader::Settings` for a complete list of possible configurations.

Subprocesses :

- Feeder : connect to crypto exchanges public and private data streams
- Logger : log feeder data to disk
- OrderManager : manage strategies order requests, sync and manage orders.
- Strategies : run trading bots
- BalanceReporter : report account asset balances

##### Db Tool

Interact with key value stores in a consistent manner without relying on specific external tools.

##### Model Loader

Load models for strategies from archived data streams

##### Backtest

Backtest indicators, models and strategies from historical data in a controlled environment, logging events, and
plotting data.

#### Architecture specifications

- Broadcasting of exchange data can be done with queues using NATS
- Each trader server embed their own graphql and rest APIs
- The reference key value store for time series data is Rocksdb
- The reference sql store for data is SQLite
- Currently, only one instance of order manager per exchange is allowed for a given database directory
- Metrics are pushed to prometheus using pushgateway
- Loggers flush and sync every event to disk
- A web interface is provided at [ui](../ui)
- Each strategy is given a single key value store for persistence
