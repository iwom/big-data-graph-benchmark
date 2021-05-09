import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkGraphxTest extends App {
  type FilePath = String

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    args(0) match {
      case "pagerank" => pageRank(args(1), args(2), args(3).toInt)
      case "degrees" => degrees(args(1), args(2))
      case "triangles" => triangles(args(1), args(2))
      case "sssp" => sssp(args(1), args(2))
    }
  }

  def pageRank(filePath: FilePath, outFilePath: FilePath, numIterations: Int): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphx | pagerank | " + filePath).getOrCreate()
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
    spark.close()
  }

  def sssp(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphx | sssp | " + filePath).getOrCreate()
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
    spark.close()
  }

  def degrees(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphx | degrees | " + filePath).getOrCreate()
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

    val output = graph
      .outDegrees

    output.collect().foreach(println)
    spark.close()
  }

  def triangles(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphx | triangles | " + filePath).getOrCreate()
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

    val graph = Graph(vertices, edges).convertToCanonicalEdges()
    val triangles = graph.triangleCount()
    // use -> graph.degrees for degree distribution
    triangles.vertices.foreach(println)
    triangles.edges.foreach(println)
    triangles.triplets.foreach(println)
    spark.close()
  }
}
