package com.nielsen.spark.examples.scala.wordcount2.process

import com.nielsen.spark.examples.scala.wordcount2.process.Processor.process
import com.nielsen.spark.examples.scala.wordcount2.util.SparkUtil.{init, stop}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Main extends Logging {

  def main(args: Array[String]) {
    logInfo("Wordcount Started")

    val spark: SparkSession = init()

    process(spark.sparkContext, args(0), args(1))

    stop()

    logInfo("Wordcount Completed")
  }

}
