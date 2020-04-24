#!/bin/sh

BASEDIR=$(dirname "$0")

scp -r -i ~/.ssh/btc "$BASEDIR/data/naive_pair_trading_model_$1_$2" root@167.71.204.126:/root/database/naive_pair_trading
