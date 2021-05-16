#!/bin/bash

# Run on master node:
# reduced
# spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphframes.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/pagerank/"$1" 30
# spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphframes.jar sssp hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/sssp/"$1"
# spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphframes.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/degrees/"$1"
# spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-graphframes.jar triangles hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/triangles/"$1"

# full
spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster spark-graphframes.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/pagerank/"$1" 30
spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster spark-graphframes.jar sssp hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/sssp/"$1"
spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster spark-graphframes.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/degrees/"$1"
spark-submit --class SparkGraphframesTest --master yarn --deploy-mode cluster spark-graphframes.jar triangles hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/graphframes/triangles/"$1"
