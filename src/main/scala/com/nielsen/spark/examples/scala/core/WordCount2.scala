package com.nielsen.spark.examples.scala.core

import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount2 extends Logging {
  def main(args: Array[String]) {
    logInfo("WordCount Started")

    val sw: StopWatch = new StopWatch()
    sw.start()

    if (args.length < 2) {
      System.err.println("Usage: WordCount2 <input path> <output path>")
      System.exit(1)
    }

    val spark: SparkSession = init("Spark Scala WordCount2")

    run(spark, args(0), args(1))

    spark.stop()

    sw.stop()

    logInfo(s"WordCount Completed - Duration $sw")
  }

  def init(appName: String): SparkSession = {
    SparkSession.builder().appName(appName).getOrCreate()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val lines: RDD[String] = extract(spark, inputPath)
    val counts: RDD[(String, Int)] = transform(lines)
    load(counts, outputPath)
  }

  def extract(spark: SparkSession, inputPath: String): RDD[String] = {
    val lines: RDD[String] = spark.read.textFile(inputPath).rdd
    lines
  }

  def transform(lines: RDD[String]): RDD[(String, Int)] = {
    val words: RDD[String] = lines.flatMap(line => line.split(" "))
    val ones: RDD[(String, Int)] = words.map(word => (word, 1))
    val counts: RDD[(String, Int)] = ones.reduceByKey(_ + _)
    counts
  }

  def load(counts: RDD[(String, Int)], outputPath: String): Unit = {
    counts.saveAsTextFile(outputPath)
  }
}