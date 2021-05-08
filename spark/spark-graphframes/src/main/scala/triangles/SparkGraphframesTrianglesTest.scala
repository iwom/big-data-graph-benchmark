package triangles

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphframesTrianglesTest extends App {
  type FilePath = String

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath: FilePath = args(0)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    triangles(spark, filePath)

    sc.stop()
    spark.close()
  }

  def triangles(spark: SparkSession, filePath: FilePath): Unit = {
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
    triangles.show(truncate = false)
  }
}
