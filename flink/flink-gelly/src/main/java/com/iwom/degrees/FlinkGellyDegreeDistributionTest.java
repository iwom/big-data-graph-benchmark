package com.iwom.degrees;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class FlinkGellyDegreeDistributionTest {
  public static void main(String[] args) {
    final String filePath = args[0];
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    try {
      degrees(env, filePath);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void degrees(ExecutionEnvironment env, String filePath) throws Exception {
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
    result.print();
  }
}

