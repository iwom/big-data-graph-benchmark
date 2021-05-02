package com.iwom.pagerank;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.types.NullValue;

public class FlinkGellyPageRankTest {
  public static void main(String[] args) {
    final String filePath = args[0];
    final Integer numIterations = Integer.parseInt(args[1]);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    try {
      pageRank(env, filePath, numIterations);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void pageRank(ExecutionEnvironment env, String filePath, Integer numInterations) throws Exception {
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
    result.print();
  }
}
