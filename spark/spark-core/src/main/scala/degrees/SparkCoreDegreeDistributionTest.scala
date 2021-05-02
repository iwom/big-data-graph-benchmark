package com.iwom
package degrees

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkCoreDegreeDistributionTest extends App {
  type FilePath = String

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath: FilePath = args(0)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    degrees(spark, filePath)

    sc.stop()
    spark.close()
  }

  def degrees(spark: SparkSession, filePath: FilePath): Unit = {
    val lines = spark.read.textFile(filePath).rdd
    val links: RDD[(String, Iterable[String])] = lines
      .map { line =>
        val parts = line.split("\\s+")
        (parts(0), parts(1))
      }
      .distinct()
      .groupByKey()
      .cache()

    val output = links
      .mapValues(_.size)

    output.collect().foreach(println)
  }
}
