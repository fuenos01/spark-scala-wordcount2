package com.nielsen.spark.examples.scala.core

import com.nielsen.spark.examples.scala.test.SparkTestUtil
import org.scalatest.{BeforeAndAfter, FunSuite}

class WordCount2MainTest extends FunSuite with BeforeAndAfter {
  test("mainTest") {
    val inputPath = "./data/"
    val outputPath = "./tmp/output/"

    WordCount2.main(Array(inputPath, outputPath))
  }

  before {
    SparkTestUtil.initTestEnv()
  }
}