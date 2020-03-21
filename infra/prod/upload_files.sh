#!/bin/sh

BASEDIR=$(dirname "$0")

scp -i ~/.ssh/btc -r $BASEDIR root@159.89.190.222:/root/box
