package pagerank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkGraphxPageRankTest extends App {
  type FilePath = String

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath: FilePath = args(0)
    val numIterations: Int = args(1).toInt

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    pageRank(spark, filePath, numIterations)

    sc.stop()
    spark.close()
  }

  def pageRank(spark: SparkSession, filePath: FilePath, numIterations: Int): Unit = {
    val lines = spark.read.textFile(filePath).rdd
    val edges: RDD[Edge[Any]] = lines
      .map { line =>
        val parts = line.split("\\s+")
        Edge(parts(0).toLong, parts(1).toLong)
      }
    val vertices: RDD[(VertexId, Any)] = edges
      .flatMap(edge => Seq(edge.srcId, edge.dstId))
      .distinct()
      .map((_, null))

    val graph = Graph(vertices, edges)

    val output = graph.pageRank(0.0001).vertices
    output.collect().foreach(tuple => println(tuple._1 + " has rank: " + tuple._2))
  }
}
