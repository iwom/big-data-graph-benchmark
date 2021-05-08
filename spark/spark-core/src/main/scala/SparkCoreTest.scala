package com.iwom

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkCoreTest extends App {
  type FilePath = String

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    args(0) match {
      case "pagerank" => pageRank(args(1), args(2).toInt, args(3))
      case "degrees" => degrees(args(1), args(2))
    }
  }

  def pageRank(filePath: FilePath, numIterations: Int, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-core | pagerank").getOrCreate()
    val lines = spark.read.textFile(filePath).rdd
    val links: RDD[(String, Iterable[String])] = lines
      .map { line =>
        val parts = line.split("\\s+")
        (parts(0), parts(1))
      }
      .distinct()
      .groupByKey()
      .cache()

    var ranks: RDD[(String, Double)] = links.mapValues(_ => 1.0)

    for (_ <- 1 to numIterations) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.saveAsObjectFile(outFilePath)
    spark.close()
  }

  def degrees(filePath: FilePath, outFilePath: FilePath): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("spark-core | degrees").getOrCreate()
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

    output.saveAsObjectFile(outFilePath)
    spark.close()
  }
}
