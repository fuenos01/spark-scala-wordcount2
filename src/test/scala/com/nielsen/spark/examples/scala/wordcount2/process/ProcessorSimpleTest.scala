package com.nielsen.spark.examples.scala.wordcount2.process

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.util.matching.Regex

class ProcessorSimpleTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test extract function") {
    val input_file: String = "src/test/resources/data/data.txt"

    val input_rdd: RDD[String] = Processor.extract(sc, input_file)

    val expected_list: List[String] = List("this is a test", "this is a test", "this is a test", "this is a test")
    val expected_rdd: RDD[String] = sc.parallelize(expected_list)

    assert(input_rdd.count === 4)
    assertRDDEquals(expected_rdd, input_rdd)
  }

  test("test transform function") {
    val input_list: List[String] = List("this is a test", "this is a test", "this is a test", "this is a test")
    val input_rdd: RDD[String] = sc.parallelize(input_list)

    val output_rdd: RDD[(String, Int)] = Processor.transform(input_rdd)

    val expected_list: List[(String, Int)] = List(("this", 4), ("is", 4), ("a", 4), ("test", 4))
    val expected_rdd: RDD[(String, Int)] = sc.parallelize(expected_list)

    assert(output_rdd.count === 4)
    assertRDDEquals(expected_rdd, output_rdd)
  }

  test("test load function") {
    val output_list: List[(String, Int)] = List(("this", 4), ("is", 4), ("a", 4), ("test", 4))
    val output_rdd: RDD[(String, Int)] = sc.parallelize(output_list)

    val output_dir = "tmp/output"

    Processor.load(output_rdd, output_dir)

    val actual_rdd: RDD[String] = sc.textFile(output_dir)

    assert(output_rdd.count === actual_rdd.count)

    val expected_list: List[String] = List("(this,4)", "(is,4)", "(a,4)", "(test,4)")
    val expected_rdd: RDD[String] = sc.parallelize(expected_list)

    assertRDDEquals(expected_rdd, actual_rdd)
  }

  test("test process function 1") {
    val input_file = "src/test/resources/data/data.txt"
    val output_dir = "tmp/output"

    Processor.process(sc, input_file, output_dir)

    val input_rdd: RDD[String] = sc.textFile(input_file)
    val output_rdd: RDD[String] = sc.textFile(output_dir)

    assert(input_rdd.count === output_rdd.count)

    val expected_list: List[String] = List("(this,4)", "(is,4)", "(a,4)", "(test,4)")
    val expected_rdd: RDD[String] = sc.parallelize(expected_list)

    assertRDDEquals(expected_rdd, output_rdd)
  }

  test("test process function 2") {
    val input_file = "src/test/resources/data/data.txt"
    val output_dir = "tmp/output"

    Processor.process(sc, input_file, output_dir)

    val input_rdd: RDD[String] = sc.textFile(input_file)
    val output_rdd: RDD[String] = sc.textFile(output_dir)

    assert(input_rdd.count === output_rdd.count)

    val actual_rdd: RDD[(String, Int)] = output_rdd.map(s => {
      val tupleRegex: Regex = """\((\w+),(\d+)\)""".r
      val t: (String, Int) = s match {
        case tupleRegex(word, freq) => (word, freq.toInt)
      }
      t
    })

    val expected_list: List[(String, Int)] = List(("this", 4), ("is", 4), ("a", 4), ("test", 4))
    val expected_rdd: RDD[(String, Int)] = sc.parallelize(expected_list)

    assertRDDEquals(expected_rdd, actual_rdd)
  }

}
