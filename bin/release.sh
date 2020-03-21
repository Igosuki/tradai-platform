#!/bin/sh

scp  -i ~/.ssh/btc cargo-target/x86_64-unknown-linux-musl/release/trader root@159.89.190.222:/root/trader.staged
ssh -i ~/.ssh/btc root@159.89.190.222 'cp trader trader.bak && systemctl stop trader.service && cp trader.staged trader && systemctl start trader'
