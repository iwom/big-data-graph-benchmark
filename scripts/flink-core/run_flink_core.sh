#!/bin/bash

# Run on master node:
flink run -m yarn-cluster flink-core.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/core/degrees/"$1"
flink run -m yarn-cluster flink-core.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/core/pagerank/"$1" 30