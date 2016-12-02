import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import utils.PathFinder

/**
  * Created by edwardzhu on 2016/11/19.
  */
object JoinDatasets {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sc = SparkContext.getOrCreate()
    val sql = new HiveContext(sc)

    val genDayOfWeek = udf((date : String) =>
      LocalDate
        .parse(date, ISODateTimeFormat.basicDate())
        .dayOfWeek()
        .get()
    )

    val keys = sql.sql("select * from keys_boro")

    val weather = sql.sql("select * from weather")
    val collision = sql
      .sql("select date, hour, boro, count(*) as count, sum(kill) as kill, sum(wound) as wound from collision_boro group by date, hour, boro")
    val games = sql.sql("select date, count as ngames from ngames")

    collision.printSchema()


    val keys_full = keys
      .withColumn("dayofweek", genDayOfWeek(keys.col("date")))
      .withColumn("month", keys.col("date").substr(5, 2).cast(IntegerType))

    val weather_l = weather.withColumn("visibility", log(weather.col("visibility")))

    val collision_weather = keys_full
      .join(collision, Seq("date", "hour", "boro"), "left_outer")
      .join(weather_l, Seq("date", "hour"))
      .join(games, Seq("date"), "left_outer")
      .na.fill(0)

    collision_weather.printSchema()

    // keys.take(10).map(println)
    collision_weather.take(10).map(println)

    collision_weather.write.parquet(PathFinder.getPath("collision_weather_games.parquet"))


    val speed = sql.sql("select * from speed_boro")
    val total = collision_weather.join(speed, Seq("date", "hour", "boro"))

    total.write.parquet(PathFinder.getPath("total.parquet"))
  }
}
