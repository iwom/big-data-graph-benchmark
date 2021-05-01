package com.iwom
package pagerank

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkCoreTest extends App {
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
    val output = ranks.collect()
    output.foreach(tuple => println(tuple._1 + " has rank: " + tuple._2))
  }
}
