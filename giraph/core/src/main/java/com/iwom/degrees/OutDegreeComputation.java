package com.iwom.degrees;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class OutDegreeComputation extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> iterable) throws IOException {
    long edgesNo = vertex.getNumEdges();
    vertex.setValue(new DoubleWritable((double) edgesNo));
    vertex.voteToHalt();
  }
}
