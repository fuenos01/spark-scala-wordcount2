package com.nielsen.spark.examples.scala.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object SparkTestUtil {
  def initTestEnv() {
    val user_dir = System.getProperty("user.dir").replace('\\', '/')
    val spark_hive_warehouse_dir = "file:///" + user_dir + "/spark-hive"
    val log4j_file = "file:///" + user_dir + "/conf/log4j.properties"

    sys.props.put("hive.exec.scratchdir", user_dir + "/tmp")
    sys.props.put("spark.sql.warehouse.dir", spark_hive_warehouse_dir)
    sys.props.put("spark.local.dir", user_dir + "/tmp/spark/scratch")
    sys.props.put("spark.master", "local[*]")

    sys.props.put("log4j.configuration", log4j_file)

    deleteDirectory(user_dir + "/tmp")
  }

  def deleteDirectory(path: String) {
    val fs: FileSystem = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
  }
}