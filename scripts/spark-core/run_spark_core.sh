#!/bin/bash

# Run on master node:
# Usage ./run_spark_core.sh pagerank hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output 100
# Usage ./run_spark_core.sh degrees hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output

# 1) Run
spark-submit --class com.iwom.SparkCoreTest --master yarn ~/big-data-graph-benchmark/spark/spark-core/target/scala-2.12/spark-core.jar "$1" "$2" "$3" "$4"

# 2) Show result
hdfs dfs -cat "$3"/* | head -n 10