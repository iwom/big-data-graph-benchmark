package com.iwom.degrees;

import com.iwom.pagerank.Adjacency;
import com.iwom.pagerank.Page;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkCoreDegreeDistributionTest {
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

    DataSet<Adjacency> adjacency = edges
      .map(new MapFunction<Tuple2<Long, Long>, Adjacency>() {
        @Override
        public Adjacency map(Tuple2<Long, Long> edge) throws Exception {
          return new Adjacency(edge.f0, Collections.singletonList(edge.f1));
        }
      })
      .groupBy("id")
      .reduce(new ReduceFunction<Adjacency>() {
        @Override
        public Adjacency reduce(Adjacency l1, Adjacency l2) throws Exception {
          return new Adjacency(
            l1.id,
            Stream.concat(l1.neighbours.stream(), l2.neighbours.stream()).collect(Collectors.toList())
          );
        }
      });

    DataSet<Tuple2<Long, Integer> >result = adjacency.map(new MapFunction<Adjacency, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Adjacency adj) throws Exception {
        return new Tuple2<Long, Integer>(adj.id, adj.neighbours.size());
      }
    });

    result.print();

  }
}
