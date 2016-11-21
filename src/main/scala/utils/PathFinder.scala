package utils

import org.apache.spark.SparkContext

/**
  * Created by edwardzhu on 2016/11/19.
  */
object PathFinder {
  val sc: SparkContext = SparkContext.getOrCreate()

  def getDatasetPath(filename: String): String = {
    val master = sc.master
    if (master.contains("local")) {
      "datasets/" + filename
    }
    else {
      "project/" + filename
    }
  }
}
