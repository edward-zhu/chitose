import org.apache.spark.sql.SparkSession

/**
  * Created by edwardzhu on 2016/11/19.
  */

object GenerateCollisionDF {
  // 2014072807,20140728,07,11355,2,0

  case class Collision(date : String, hour : Int, zipcode : String, kill : Int, wound : Int)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Generate Collision DataFrame")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext
      .textFile("datasets/collision/*")
      .map(_.split(","))
      .map(attr => Collision(attr(1), attr(2).trim.toInt, attr(3), attr(4).trim.toInt, attr(5).trim.toInt))
      .toDF();


    df.createOrReplaceTempView("collision")
    df.write.parquet("datasets/collision.parquet")
  }
}
