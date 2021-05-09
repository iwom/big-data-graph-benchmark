#!/bin/bash

# Run on master node:

# 1) Build sources for flink-core-test
sudo apt-get install maven -y
git clone https://github.com/iwom/big-data-graph-benchmark.git || echo "big-data-graph-benchmark.git already exists"
cd ~/big-data-graph-benchmark/flink/flink-core || exit
mvn clean package
cd - || exit