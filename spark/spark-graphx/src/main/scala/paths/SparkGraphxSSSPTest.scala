package paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib.ShortestPaths

object SparkGraphxSSSPTest extends App {
  type FilePath = String

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath: FilePath = args(0)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sssp(spark, filePath)

    sc.stop()
    spark.close()
  }

  def sssp(spark: SparkSession, filePath: FilePath): Unit = {
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
    val shortestPaths = ShortestPaths.run(graph, Seq(1))
    shortestPaths
      .vertices
      .foreach(println)
  }
}
