#!/bin/bash

# Run on master node:
# Usage ./run_flink_gelly.sh pagerank hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output 100
# Usage ./run_flink_gelly.sh degrees hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output
# Usage ./run_flink_gelly.sh triangles hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output
# Usage ./run_flink_gelly.sh sssp hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output

# 4) Run
flink run -m yarn-cluster ~/big-data-graph-benchmark/flink/flink-gelly/target/flink-gelly-1.0-SNAPSHOT.jar "$1" "$2" "$3" "$4"

# 5) Show result
hdfs dfs -cat "$3"/* | head -n 10