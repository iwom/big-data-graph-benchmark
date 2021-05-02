package com.iwom.pagerank;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class PageRankVertexComputation extends BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {

  public static final String SUPERSTEP_COUNT = "giraph.pageRank.superstepCount";

  @Override
  public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
                      Iterable<DoubleWritable> messages) throws IOException {

    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(1.0f / getTotalNumVertices()));
    } else {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue = new DoubleWritable( 0.15f / getTotalNumVertices() + 0.85f * sum);
      vertex.setValue(vertexValue);
    }

    if (getSuperstep() < (long)this.getConf().getInt(SUPERSTEP_COUNT, 0)) {
      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() / edges));
    }
    else {
      vertex.voteToHalt();
    }
  }
}