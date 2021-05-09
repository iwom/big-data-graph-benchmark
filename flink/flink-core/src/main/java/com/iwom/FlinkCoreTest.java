package com.iwom;

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

public class FlinkCoreTest {
  public static void main(String[] args) {
    try {
      switch (args[0]) {
        case "pagerank":
          pageRank(args[1], args[2], Integer.parseInt(args[3]));
          break;
        case "degrees":
          degrees(args[1], args[2]);
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
    Double randomJump = 0.15;

    DataSet<Tuple2<Long, Long>> edges = env.readTextFile(filePath)
      .map(new MapFunction<String, Tuple2<Long, Long>>() {
        @Override
        public Tuple2<Long, Long> map(String line) throws Exception {
          String[] elements = line.split("\\s+");
          return new Tuple2<>(Long.parseLong(elements[0]), Long.parseLong(elements[1]));
        }
      })
      .distinct();

    DataSet<Page> pages = edges
      .flatMap(new FlatMapFunction<Tuple2<Long, Long>, Page>() {
        @Override
        public void flatMap(Tuple2<Long, Long> edge, Collector<Page> out) {
          out.collect(new Page(edge.f0, 1.0));
          out.collect(new Page(edge.f1, 1.0));
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

    IterativeDataSet<Page> initialPages = pages.iterate(numInterations);

    DataSet<Page> iteration = initialPages
      .join(adjacency)
      .where("id")
      .equalTo("id")
      .with(new FlatJoinFunction<Page, Adjacency, Page>() {
        @Override
        public void join(Page page, Adjacency adjacency, Collector<Page> collector) throws Exception {
          Double rankPerTarget = 0.85 * page.rank / adjacency.neighbours.size();
          collector.collect(new Page(page.id, randomJump));
          for (Long neighbour : adjacency.neighbours) {
            collector.collect(new Page(neighbour, rankPerTarget));
          }
        }
      })
      .groupBy("id")
      .reduce(new ReduceFunction<Page>() {
        @Override
        public Page reduce(Page p1, Page p2) throws Exception {
          return new Page(p1.id, p1.rank + p2.rank);
        }
      });

    DataSet<Page> result = initialPages.closeWith(iteration);
    result.writeAsText(outFilePath);
    env.execute("flink-core | pagerank | " + filePath);
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

    result.writeAsText(outFilePath);
    env.execute("flink-core | degrees | " + filePath);
  }
}
