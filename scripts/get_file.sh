#!/bin/bash

# Run on master node:
# Usage ./get_file.sh <bucket-name> <in-bucket-path> <target-hdfs-file-name>
# Example ./get_file.sh dataproc-testing-bucket-iomi email-EuAll.txt email.graph

# 1) Download input graph
gsutil cp gs://"$1"/"$2" graph.txt

# 2) Move graph to hdfs
hdfs dfs -put ~/graph.txt /user/"$3"
