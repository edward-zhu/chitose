import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext;
import utils.PathFinder

/**
  * Created by edwardzhu on 2016/11/19.
  */
object JoinDatasets {
  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val sql = new HiveContext(sc);


    val weather = sql.sql("select * from weather")
    val collision = sql.sql("select datehour, count(*), sum(kill), sum(wound) from collision group by datehour, zipcode")
    val games = sql.sql("select * from games")

    val collision_weather = collision
      .join(weather, Seq("date", "hour"))
      .join(games, Seq("date"))
      .withColumnRenamed("count", "ngames")

    collision_weather.write.parquet(PathFinder.getDatasetPath("collision_weather_games.parquet"))


    val speed = sql.sql("select * from speed")
    val total = collision_weather.join(speed, Seq("date", "hour", "zipcode"))

    total.write.parquet(PathFinder.getDatasetPath("total.parquet"))
  }
}
