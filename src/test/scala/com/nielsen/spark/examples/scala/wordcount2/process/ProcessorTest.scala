package com.nielsen.spark.examples.scala.wordcount2.process

import com.nielsen.spark.examples.scala.wordcount2.util.SparkUtil.{init, stop}
import com.nielsen.spark.test.SparkTestUtil.initTestEnv
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FunSuite}

class ProcessorTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _

  test("Processor") {
    val input_file = "src/test/resources/data/data.txt"
    val output_dir = "tmp/output"

    Processor.process(sc, input_file, output_dir)

    val input_array: Array[String] = sc.textFile(input_file).collect
    val output_array: Array[String] = sc.textFile(output_dir).collect

    assert(input_array.length === output_array.length)

    val expected_array:Array[String] = Array("(this,4)", "(is,4)", "(a,4)", "(test,4)")

    assert(expected_array === output_array)
  }

  before {
    initTestEnv()
    val spark = init()
    sc = spark.sparkContext
  }

  after {
    stop()
  }

}
