package com.nielsen.spark.examples.scala.wordcount2.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtil {
  var spark: SparkSession = _

  def init(): SparkSession = {
    spark = SparkSession
      .builder()
      .master("local")
      .appName("Wordcount Example")
      .getOrCreate()

    spark
  }

  def stop(): Unit = {
    spark.stop()
  }

  def getSparkSession: SparkSession = {
    spark
  }

  def getSparkContext: SparkContext = {
    spark.sparkContext
  }
}