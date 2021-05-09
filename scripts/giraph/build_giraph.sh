#!/bin/bash

# Run on master node:

# 1) Build sources for Apache Giraph from fork
sudo apt-get install maven -y
git clone https://github.com/iwom/giraph.git
export GIRAPH_HOME=~/giraph
cd $GIRAPH_HOME || exit
git checkout feature/triangle-count
mvn -Phadoop_2 -Dhadoop.version=2.5.1 -DskipTests package
cd - || exit