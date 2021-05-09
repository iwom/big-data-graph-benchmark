#!/bin/bash

# Run on master node:
# Usage ./run_spark_graphframes.sh pagerank hdfs://spark-m:8020/user/input.txt hdfs://spark-m:8020/user/output 100
# Usage ./run_spark_graphframes.sh degrees hdfs://spark-m:8020/user/input.txt hdfs://spark-m:8020/user/output
# Usage ./run_spark_graphframes.sh triangles hdfs://spark-m:8020/user/input.txt hdfs://spark-m:8020/user/output
# Usage ./run_spark_graphframes.sh sssp hdfs://spark-m:8020/user/email.graph hdfs://spark-m:8020/user/output

# 1) Run
spark-submit --class SparkGraphframesTest --master yarn ~/spark-graphframes.jar "$1" "$2" "$3" "$4"

# 2) Show result
hdfs dfs -cat "$3"/* | head -n 10