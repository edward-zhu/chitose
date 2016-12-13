package utils

import org.apache.spark.SparkContext

/**
  * Created by edwardzhu on 2016/11/19.
  */
object PathFinder {
  val sc: SparkContext = SparkContext.getOrCreate()

  var customPath : String = null

  def setCustomPath(path: String): Unit = {
    customPath = path;
  }

  def getPath(filename: String): String = {
    val master = sc.master
    if (customPath != null) {
      customPath + filename
    }
    else if (master.contains("local")) {
      "datasets/" + filename
    }
    else {
      "project/" + filename
    }
  }
}
