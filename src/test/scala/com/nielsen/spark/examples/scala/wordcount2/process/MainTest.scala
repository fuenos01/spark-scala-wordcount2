package com.nielsen.spark.examples.scala.wordcount2.process

import com.nielsen.spark.test.SparkTestUtil.initTestEnv
import org.scalatest.{BeforeAndAfter, FunSuite}

class MainTest extends FunSuite with BeforeAndAfter {

  test("Main") {
    val input_file = "src/test/resources/data/data.txt"
    val output_dir = "tmp/output"

    Main.main(Array(input_file, output_dir))
  }

  before {
    initTestEnv()
  }

  after {
  }

}
