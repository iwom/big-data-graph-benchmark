#!/bin/bash

#reduced
# spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphx.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/pagerank/"$1" 30
# spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphx.jar sssp hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/sssp/"$1"
# spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphx.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/degrees/"$1"
# spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphx.jar triangles hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/triangles/"$1"
#full
spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster spark-graphx.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/pagerank/"$1" 30
spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster spark-graphx.jar sssp hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/sssp/"$1"
spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster spark-graphx.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/degrees/"$1"
spark-submit --class SparkGraphxTest --master yarn --deploy-mode cluster spark-graphx.jar triangles hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphx/triangles/"$1"
