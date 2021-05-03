package com.iwom.triangles;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.graph.library.TriangleEnumerator;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class FlinkGellyTrianglesTest {
  public static void main(String[] args) {
    final String filePath = args[0];
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    try {
      triangles(env, filePath);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void triangles(ExecutionEnvironment env, String filePath) throws Exception {
    DataSet<Edge<LongValue, DoubleValue>> edges = env.readTextFile(filePath)
      .map(new MapFunction<String, Edge<LongValue, DoubleValue>>() {
        @Override
        public Edge<LongValue, DoubleValue> map(String line) throws Exception {
          String[] elements = line.split("\\s+");
          return new Edge(new LongValue(Long.parseLong(elements[0])), new LongValue(Long.parseLong(elements[1])), new DoubleValue(1.0));
        }
      })
      .distinct();

    DataSet<Vertex<LongValue, DoubleValue>> vertices = edges
      .flatMap(new FlatMapFunction<Edge<LongValue, DoubleValue>, Vertex<LongValue, DoubleValue>>() {
        @Override
        public void flatMap(Edge<LongValue, DoubleValue> edge, Collector<Vertex<LongValue, DoubleValue>> collector) throws Exception {
          collector.collect(new Vertex(edge.f0, new DoubleValue(1.0)));
          collector.collect(new Vertex(edge.f1, new DoubleValue(1.0)));
        }
      })
      .distinct();

    Graph<LongValue, DoubleValue, DoubleValue> graph = Graph.fromDataSet(vertices, edges, env);
    LocalClusteringCoefficient coef = new LocalClusteringCoefficient();
    DataSet<LocalClusteringCoefficient.Result> result = coef.run(graph);

    result
      .map(new MapFunction<LocalClusteringCoefficient.Result, StringValue>() {
        @Override
        public StringValue map(LocalClusteringCoefficient.Result result) throws Exception {
          return new StringValue(result.toPrintableString());
        }
      })
      .print();
  }
}
