package com.nielsen.spark.examples.scala.wordcount2.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtil {

  def deleteDirectory(path: String) {
    val fs: FileSystem = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
  }

}
