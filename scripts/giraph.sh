#!/bin/bash

# Run on master node:

# 1) Build Giraph from fork
sudo apt-get install maven -y
git clone https://github.com/iwom/giraph.git
export GIRAPH_HOME=~/giraph
cd $GIRAPH_HOME || exit
git checkout feature/triangle-count
mvn -Phadoop_2 -Dhadoop.version=2.5.1 -DskipTests package
cd - || exit

# 2) Download input graph
gsutil cp gs://dataproc-testing-bucket-iomi/p2p-Gnutella09.txt graph.txt

# 3) Move graph to hdfs
hdfs dfs -put ~/graph.txt /user/graph.txt
IP_ADDR=$(hostname --ip-address)
hadoop jar ~/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker="$IP_ADDR" org.apache.giraph.examples.TriangleCensusComputation -eif org.apache.giraph.examples.io.formats.DoubleDoubleFloatDoubleEdgeInputFormat -eip /user/graph.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/output -w 2 -ca giraph.SplitMasterWorker=false