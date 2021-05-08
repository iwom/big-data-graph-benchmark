#!/bin/bash

# Run on master node:

# 1) Run
spark-submit --class com.iwom.SparkCoreTest --master yarn ~/big-data-graph-benchmark/spark/spark-core/target/scala-2.12/spark-core.jar pagerank hdfs://aaa 1

# 2) Show result
# hdfs dfs -cat /user/"$4"/* | sort -k2 -n | tail -n 10