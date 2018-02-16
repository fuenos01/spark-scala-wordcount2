package com.nielsen.spark.examples.scala.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object SparkTestUtil {
  def initTestEnv() {
    val userDir: String = System.getProperty("user.dir").replace('\\', '/')
    val tmpDir: String = s"file:///$userDir/tmp"

    val sparkHiveWarehouseDir: String = s"file:///$userDir/tmp/spark-hive"
    val log4jFile: String = s"file:///$userDir/conf/log4j.properties"

    sys.props.put("hive.exec.scratchdir", s"$userDir/tmp")
    sys.props.put("spark.sql.warehouse.dir", sparkHiveWarehouseDir)

    sys.props.put("spark.local.dir", s"$userDir/tmp/spark/scratch")
    sys.props.put("spark.master", "local[*]")

    sys.props.put("log4j.configuration", log4jFile)

    deleteDirectory(s"$userDir/tmp")
  }

  def deleteDirectory(path: String) {
    val fs: FileSystem = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
  }
}