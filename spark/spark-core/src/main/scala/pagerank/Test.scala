package com.iwom
package pagerank

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Test extends App {
  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath: String = args(0)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext
  }
}
