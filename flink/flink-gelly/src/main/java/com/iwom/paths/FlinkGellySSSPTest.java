package com.iwom.paths;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class FlinkGellySSSPTest {
  public static void main(String[] args) {
    final String filePath = args[0];
    final Integer numIterations = Integer.parseInt(args[1]);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    try {
      sssp(env, filePath, numIterations);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void sssp(ExecutionEnvironment env, String filePath, Integer numInterations) throws Exception {
    DataSet<Edge<Long, Double>> edges = env.readTextFile(filePath)
      .map(new MapFunction<String, Edge<Long, Double>>() {
        @Override
        public Edge<Long, Double> map(String line) throws Exception {
          String[] elements = line.split("\\s+");
          return new Edge(Long.parseLong(elements[0]), Long.parseLong(elements[1]), 1.0);
        }
      })
      .distinct();

    DataSet<Vertex<Long, Double>> vertices = edges
      .flatMap(new FlatMapFunction<Edge<Long, Double>, Vertex<Long, Double>>() {
        @Override
        public void flatMap(Edge<Long, Double> edge, Collector<Vertex<Long, Double>> collector) throws Exception {
          collector.collect(new Vertex<Long, Double>(edge.f0, 1.0));
          collector.collect(new Vertex<Long, Double>(edge.f1, 1.0));
        }
      })
      .distinct();

    Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);
    SingleSourceShortestPaths sssp = new SingleSourceShortestPaths(1L, numInterations);

    DataSet<Vertex<Long, Double>> result = sssp.run(graph);
    result.print();
  }
}
