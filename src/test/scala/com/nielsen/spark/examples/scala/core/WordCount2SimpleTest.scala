package com.nielsen.spark.examples.scala.core

import com.nielsen.spark.examples.scala.test.SparkTestUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class WordCount2SimpleTest extends FunSuite with BeforeAndAfter {
  var spark: SparkSession = _

  test("runTest") {
    val inputPath = "./data/"
    val outputPath = "./tmp/output/"

    WordCount2.run(spark, inputPath, outputPath)

    val inputRecords: Array[String] = spark.read.textFile(inputPath).rdd.collect
    val actualOutputRecords: Array[String] = spark.read.textFile(outputPath).rdd.collect
    assert(inputRecords.length === actualOutputRecords.length)

    val expectedOutputRecords: Array[String] = Array("(this,4)", "(is,4)", "(a,4)", "(test,4)")
    assert(expectedOutputRecords === actualOutputRecords)
  }

  before {
    SparkTestUtil.initTestEnv()

    spark = WordCount2.init("Spark Scala WordCount2")
  }

  after {
    spark.stop()
  }
}