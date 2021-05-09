#!/bin/bash

# Run on master node:
# Usage ./run_spark_graphframes.sh pagerank hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output 100
# Usage ./run_spark_graphframes.sh degrees hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output
# Usage ./run_spark_graphframes.sh triangles hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output
# Usage ./run_spark_graphframes.sh sssp hdfs://flink-m:8020/user/input.txt hdfs://flink-m:8020/user/output

# 1) Run
spark-submit --class com.iwom.SparkGraphframesTest --master yarn ~/big-data-graph-benchmark/spark/spark-graphframes/target/scala-2.12/spark-graphframes.jar "$1" "$2" "$3" "$4"

# 2) Show result
hdfs dfs -cat "$3"/* | head -n 10