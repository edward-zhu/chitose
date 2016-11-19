import org.apache.spark.sql.SparkSession

// 20120101    0    26    0    16093    100    0    0
object GenerateWeatherDF {

  case class HourlyWeather(date: String, hour: Int, wind: Int, precip: Int, visibility: Int,
    temp: Int, snowDepth: Int, weatherType: Int)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark Test")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext
      .textFile("datasets/weather.csv")
      .map(_.split("\t"))
      .map(attr => HourlyWeather(attr(0),
          attr(1).trim.toInt,
          attr(2).trim.toInt,
          attr(3).trim.toInt,
          attr(4).trim.toInt,
          attr(5).trim.toInt,
          attr(6).trim.toInt,
          attr(7).trim.toInt))
      .toDF()
    df.createOrReplaceTempView("weather")

    df.write.parquet("datasets/weather.parquet")
  }
}


