#!/bin/bash

# Run on master node:
#reduced
# spark-submit --class com.iwom.SparkCoreTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-core.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/core/pagerank/"$1" 30
# spark-submit --class com.iwom.SparkCoreTest --master yarn --deploy-mode cluster --executor-memory 2600m --driver-memory 2600m --executor-cores 1 spark-core.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/core/degrees/"$1"
#full
spark-submit --class com.iwom.SparkCoreTest --master yarn --deploy-mode cluster spark-core.jar pagerank hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/core/pagerank/"$1" 30
spark-submit --class com.iwom.SparkCoreTest --master yarn --deploy-mode cluster spark-core.jar degrees hdfs://${HOSTNAME}:8020/user/"$1" hdfs://${HOSTNAME}:8020/user/out/core/degrees/"$1"
