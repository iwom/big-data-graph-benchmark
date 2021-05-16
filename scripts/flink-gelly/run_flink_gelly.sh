#!/bin/bash

flink run -m yarn-cluster flink-gelly.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/gelly/degrees/"$1"
flink run -m yarn-cluster flink-gelly.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/gelly/pagerank/"$1" 30
flink run -m yarn-cluster flink-gelly.jar triangles hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/gelly/triangles/"$1"
flink run -m yarn-cluster flink-gelly.jar sssp hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/gelly/sssp/"$1"