package com.iwom;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class FlinkGellyTest {
  public static void main(String[] args) {
    try {
      switch (args[0]) {
        case "pagerank":
          pageRank(args[1], args[2], Integer.parseInt(args[3]));
          break;
        case "degrees":
          degrees(args[1], args[2]);
          break;
        case "sssp":
          sssp(args[1], args[2]);
          break;
        case "triangles":
          triangles(args[1], args[2]);
          break;
        default:
          throw new RuntimeException("Unknown algorithm " + args[0]);
      }
    } catch (Exception e) {
      System.out.println("Exception found: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public static void pageRank(String filePath, String outFilePath, Integer numInterations) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Tuple2<Long, Long>> edges = env.readTextFile(filePath)
      .map(new MapFunction<String, Tuple2<Long, Long>>() {
        @Override
        public Tuple2<Long, Long> map(String line) throws Exception {
          String[] elements = line.split("\\s+");
          return new Tuple2<>(Long.parseLong(elements[0]), Long.parseLong(elements[1]));
        }
      })
      .distinct();

    Graph<Long, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env);
    PageRank pageRank = new PageRank(0.85, numInterations);

    DataSet<PageRank.Result> result = pageRank.run(graph);
    result.writeAsText(outFilePath);

    env.execute("flink-gelly | pagerank | " + filePath);
    System.out.println("flink-gelly | pagerank | " + filePath);

  }

  public static void degrees(String filePath, String outFilePath) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Tuple2<Long, Long>> edges = env.readTextFile(filePath)
      .map(new MapFunction<String, Tuple2<Long, Long>>() {
        @Override
        public Tuple2<Long, Long> map(String line) throws Exception {
          String[] elements = line.split("\\s+");
          return new Tuple2<>(Long.parseLong(elements[0]), Long.parseLong(elements[1]));
        }
      })
      .distinct();

    Graph<Long, NullValue, NullValue> graph = Graph.fromTuple2DataSet(edges, env);
    DataSet<Tuple2<Long, LongValue>> result = graph.outDegrees();
    result.writeAsText(outFilePath);

    env.execute("flink-gelly | degrees | " + filePath);
    System.out.println("flink-gelly | degrees | " + filePath);
  }

  public static void sssp(String filePath, String outFilePath) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
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
    SingleSourceShortestPaths sssp = new SingleSourceShortestPaths(1L, Integer.MAX_VALUE);

    DataSet<Vertex<Long, Double>> result = sssp.run(graph);
    result.writeAsText(outFilePath);
    env.execute("flink-gelly | sssp | " + filePath);
    System.out.println("flink-gelly | sssp | " + filePath);
  }

  public static void triangles(String filePath, String outFilePath) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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
      }).writeAsText(outFilePath);

    env.execute("flink-gelly | triangles | " + filePath);
    System.out.println("flink-gelly | triangles | " + filePath);
  }
}
