#!/bin/sh

wget https://github.com/prometheus/prometheus/releases/download/v2.16.0/prometheus-2.16.0.linux-amd64.tar.gz
tar -xf prometheus-2.16.0.linux-amd64.tar.gz
mv prometheus-2.16.0.linux-amd64/ prometheus/

mkdir -p ~/prometheus/data
