package com.iwom.degrees;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class OutDegreeComputation extends BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {
  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex, Iterable<DoubleWritable> iterable) throws IOException {
    long edgesNo = vertex.getNumEdges();
    vertex.setValue(new DoubleWritable((double) edgesNo));
    vertex.voteToHalt();
  }
}
