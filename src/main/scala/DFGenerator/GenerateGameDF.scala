import org.apache.spark.sql.SparkSession

/**
  * Created by edwardzhu on 2016/11/19.
  */

object GenerateGameDF {

  case class Game(date : String, count : Int)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Generate Game DataFrame")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext
      .textFile("datasets/games.csv")
      .map(_.split(","))
      .map(attr => Game(attr(0), attr(1).trim.toInt))
      .toDF();
    

    df.createOrReplaceTempView("game")
    df.write.parquet("datasets/games.parquet")
  }
}
