#!/bin/bash

# Run on master node:
# Usage ./run_spark_graphx.sh pagerank hdfs://spark-m:8020/user/input.txt hdfs://spark-m:8020/user/output 100
# Usage ./run_spark_graphx.sh degrees hdfs://spark-m:8020/user/input.txt hdfs://spark-m:8020/user/output

# 1) Run
spark-submit --class SparkGraphxTest --master yarn ~/big-data-graph-benchmark/spark/spark-core/target/scala-2.12/spark-core.jar "$1" "$2" "$3" "$4"

# 2) Show result
hdfs dfs -cat "$3"/* | head -n 10