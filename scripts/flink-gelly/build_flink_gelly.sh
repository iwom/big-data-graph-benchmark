#!/bin/bash

# Run on master node:

# 1) Build sources for flink-gelly-test
sudo apt-get install maven -y
git clone https://github.com/iwom/big-data-graph-benchmark.git
cd ~/big-data-graph-benchmark/flink/flink-gelly || exit
mvn clean package
cd - || exit