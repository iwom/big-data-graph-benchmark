package paths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphframesSSSPTest extends App {
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
    val sssp = graph.shortestPaths.landmarks(Seq(1)).run()

    sssp.show(truncate=false)
  }
}
