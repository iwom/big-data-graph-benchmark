package pagerank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphframesTest extends App {
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

    edgesDF.show()
    verticesDF.show()

    val graph = GraphFrame(verticesDF, edgesDF)
    val pageRank = graph.pageRank.resetProbability(0.15).maxIter(numIterations).run()

    pageRank.vertices.select("id", "pagerank").show()

  }
}
