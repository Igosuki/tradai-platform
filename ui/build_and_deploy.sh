#!/bin/sh

yarn build
scp -r build/* btcmon:/var/www/trader_ui
