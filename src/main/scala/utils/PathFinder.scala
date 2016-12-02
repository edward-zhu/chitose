package utils

import org.apache.spark.SparkContext

/**
  * Created by edwardzhu on 2016/11/19.
  */
object PathFinder {
  val sc: SparkContext = SparkContext.getOrCreate()

  def getPath(filename: String): String = {
    val master = sc.master
    if (master.contains("local")) {
      "datasets/" + filename
    }
    else {
      "gs://dataproc-f3a5e218-c9a9-4cf0-bc76-d83d685831b4-us/" + filename
    }
  }
}
