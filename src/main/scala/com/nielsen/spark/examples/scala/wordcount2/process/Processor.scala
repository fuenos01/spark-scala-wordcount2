package com.nielsen.spark.examples.scala.wordcount2.process

import com.nielsen.spark.examples.scala.wordcount2.util.HDFSUtil
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object Processor extends Logging {
  def process(sc: SparkContext, input_file: String, output_file: String): Unit = {
    logInfo("Wordcount Process Started")

    val input_rdd: RDD[String] = extract(sc, input_file)
    val output_rdd: RDD[(String, Int)] = transform(input_rdd)
    load(output_rdd, output_file)

    logInfo("Wordcount Process Completed")
  }

  def extract(sc: SparkContext, input_file: String): RDD[String] = {
    val rdd: RDD[String] = sc.textFile(input_file)
    rdd
  }

  def transform(input_rdd: RDD[String]): RDD[(String, Int)] = {
    val map_file: RDD[String] = input_rdd.flatMap(line => line.split(" "))
    val mapper: RDD[(String, Int)] = map_file.map(word => (word, 1))
    val output_rdd: RDD[(String, Int)] = mapper.reduceByKey(_ + _)
    output_rdd
  }

  def load(rdd: RDD[(String, Int)], output_file: String): Unit = {
    HDFSUtil.deleteDirectory(output_file)
    rdd.saveAsTextFile(output_file)
  }
}