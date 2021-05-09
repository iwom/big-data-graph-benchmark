#!/bin/bash

# Run on master node:

# 1) Build sources for flink-core-test
git clone https://github.com/iwom/big-data-graph-benchmark.git
cd ~/big-data-graph-benchmark/flink/flink-core || exit
mvn clean package
cd - || exit