#!/bin/bash

git clone https://github.com/iwom/big-data-graph-benchmark.git || echo "big-data-graph-benchmark.git already exists"
gsutil cp gs://"$1"/spark-graphframes.jar spark-graphframes.jar