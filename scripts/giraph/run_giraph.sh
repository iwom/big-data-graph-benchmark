#!/bin/bash

# Run on master node:
# Usage ./run_giraph.sh <compute-class-name> <input-format-class-name> <hdfs-input-name> <hdfs-output-name> <num-workers>
# ./run_giraph.sh TriangleCensusComputation DoubleDoubleFloatDoubleEdgeInputFormat graph.txt graph 4
# ./run_giraph.sh PageRankComputation DoubleDoubleFloatDoubleEdgeInputFormat graph.txt graph 4

# hadoop jar ~/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker="$IP_ADDR" -Djava.net.preferIPv4Stack=true org.apache.giraph.examples.TriangleCensusComputation -eif org.apache.giraph.examples.io.formats.DoubleDoubleFloatDoubleEdgeInputFormat -eip hdfs://giraph-m/user/email.graph -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op hdfs://giraph-m/user/triangles -w 2
# hadoop jar ~/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker="$IP_ADDR" -Djava.net.preferIPv4Stack=true org.apache.giraph.examples.PageRankVertexComputation -eif org.apache.giraph.examples.io.formats.LongDoubleFloatDoubleEdgeInputFormat -eip hdfs://giraph-m/user/email.graph -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op hdfs://giraph-m/user/pagerank -w 2 -ca giraph.pageRank.superstepCount=30
# hadoop jar ~/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker="$IP_ADDR" -Djava.net.preferIPv4Stack=true org.apache.giraph.examples.SimpleShortestPathsComputation -eif org.apache.giraph.examples.io.formats.LongDoubleFloatDoubleEdgeInputFormat -eip hdfs://giraph-m/user/email.graph -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op hdfs://giraph-m/user/sssp -w 2 -ca SimpleShortestPathsVertex.sourceId=1
# hadoop jar ~/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker="$IP_ADDR" -Djava.net.preferIPv4Stack=true org.apache.giraph.examples.OutDegreeComputation -eif org.apache.giraph.examples.io.formats.LongDoubleFloatDoubleEdgeInputFormat -eip hdfs://giraph-m/user/email.graph -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op hdfs://giraph-m/user/degrees -w 2

# 4) Run
IP_ADDR=$(hostname --ip-address)
hadoop jar ~/giraph/giraph-examples/target/giraph-examples-1.4.0-SNAPSHOT-for-hadoop-2.5.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner -Dmapred.job.tracker="$IP_ADDR" -Djava.net.preferIPv4Stack=true org.apache.giraph.examples."$1" -eif org.apache.giraph.examples.io.formats."$2" -eip /user/"$3" -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/"$4" -w "$5"

# 5) Show result
hdfs dfs -cat /user/"$4"/* | sort -k2 -n | tail -n 10