#!/bin/bash

# Run on master node:
# Usage ./run_flink.sh pagerank hdfs://flink-m:8082/user/input.txt hdfs://flink-m:8082/user/output 100
# Usage ./run_flink.sh degrees hdfs://flink-m:8082/user/input.txt hdfs://flink-m:8082/user/output

# 4) Run
flink run -m yarn-cluster ~/big-data-graph-benchmark/flink/flink-core/target/flink-core-1.0-SNAPSHOT.jar "$1" "$2" "$3" "$4"

# 5) Show result
hdfs dfs -cat /user/"$3"/* | head -n 10