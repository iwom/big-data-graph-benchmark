import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphframesTest extends App {
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
    val spark: SparkSession = SparkSession.builder().appName("spark-graphframes | pagerank | " + filePath).getOrCreate()
    import spark.implicits._
    val lines = spark.read.text(filePath)
    val edgesDF = lines
      .map(line => {
        val parts = line.getString(0).split("\\s+")
        (parts(0).toLong, parts(1).toLong)
      })
      .toDF("src", "dst")

    val verticesDF1 = edgesDF.select("src").distinct().toDF("id")
    val verticesDF2 = edgesDF.select("dst").distinct().toDF("id")
    val verticesDF = verticesDF1.union(verticesDF2).distinct()

    val graph = GraphFrame(verticesDF, edgesDF)
    val pageRank = graph.pageRank.resetProbability(0.15).maxIter(numIterations).run()

    pageRank.vertices.select("id", "pagerank").write.csv(outFilePath)
    spark.close()
  }

  def triangles(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphframes | triangles | " + filePath).getOrCreate()
    import spark.implicits._
    val lines = spark.read.text(filePath)
    val edgesDF = lines
      .map(line => {
        val parts = line.getString(0).split("\\s+")
        (parts(0).toLong, parts(1).toLong)
      })
      .toDF("src", "dst")

    val verticesDF1 = edgesDF.select("src").distinct().toDF("id")
    val verticesDF2 = edgesDF.select("dst").distinct().toDF("id")
    val verticesDF = verticesDF1.union(verticesDF2).distinct()

    val graph = GraphFrame(verticesDF, edgesDF)
    val triangles = graph.triangleCount.run()
    triangles.write.csv(outFilePath)
    spark.close()
  }

  def sssp(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphframes | sssp | " + filePath).master("local").getOrCreate()
    import org.apache.spark.sql.functions.col
    import spark.implicits._
    val lines = spark.read.text(filePath)
    val edgesDF = lines
      .map(line => {
        val parts = line.getString(0).split("\\s+")
        // Reversed graph for landmarks
        (parts(1).toLong, parts(0).toLong)
      })
      .toDF("src", "dst")

    val verticesDF1 = edgesDF.select("src").distinct().toDF("id")
    val verticesDF2 = edgesDF.select("dst").distinct().toDF("id")
    val verticesDF = verticesDF1.union(verticesDF2).distinct()

    val graph = GraphFrame(verticesDF, edgesDF)
    val sssp = graph.shortestPaths.landmarks(Seq(1)).run()
    val newSssp = sssp.select(col("id"), col("distances").getItem(1).as("distance"))
    newSssp.write.csv(outFilePath)
    spark.close()
  }

  def degrees(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-graphframes | degrees | " + filePath).getOrCreate()
    import spark.implicits._
    val lines = spark.read.text(filePath)
    val edgesDF = lines
      .map(line => {
        val parts = line.getString(0).split("\\s+")
        (parts(0).toLong, parts(1).toLong)
      })
      .toDF("src", "dst")

    val verticesDF1 = edgesDF.select("src").distinct().toDF("id")
    val verticesDF2 = edgesDF.select("dst").distinct().toDF("id")
    val verticesDF = verticesDF1.union(verticesDF2).distinct()

    val graph = GraphFrame(verticesDF, edgesDF)
    graph.outDegrees.write.csv(outFilePath)
    spark.close()
  }
}
