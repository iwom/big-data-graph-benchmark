#!/bin/bash

# Run on master node:
# Usage ./get_file.sh <bucket-name> <in-bucket-path> <target-hdfs-file-name>
# Example ./get_file.sh dataproc-testing-bucket-iomi email-EuAll.txt email.graph
# gsutil cp gs://dataproc-graph-processing/Amazon0302.txt Amazon0302.txt

# 1) Download input graph
gsutil cp gs://"$1"/"$2" graph.txt

# 2) Move graph to hdfs
hdfs dfs -put ~/graph.txt /user/"$3"
