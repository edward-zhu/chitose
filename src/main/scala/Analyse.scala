import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import utils.PathFinder

/**
  * Created by edwardzhu on 2016/11/21.
  */
object Analyse {
  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sc = SparkContext.getOrCreate();
    val sql = new HiveContext(sc)
    val raw = sql.read.parquet(PathFinder.getDatasetPath("total.parquet"))

    val data = raw
      .withColumn("count", raw.col("count").cast(DoubleType))
      .withColumn("kill", raw.col("kill").cast(DoubleType))
      .withColumn("wound", raw.col("wound").cast(DoubleType))

    /*
    val bucketizer = new Bucketizer()
      .setInputCol("count")
      .setOutputCol("class")
      .setSplits(Array(-0.1, 0.99, 1.99, 20))

    val bucketed = bucketizer.transform(data)

    val zero = bucketed.filter("count = 0.0")
    val one = bucketed.filter("count = 1.0")
    val more = bucketed.filter("count >= 2.0")

    println("zero class: " + zero.count())
    println("one class: " + one.count())
    println("more class: " + more.count())

    val sone = zero.sample(false, more.count().toDouble / zero.count().toDouble)
    val stwo = one.sample(false, more.count().toDouble / one.count().toDouble)

    val subsampledData = sone.unionAll(stwo).unionAll(more)

    subsampledData.groupBy("class").count().show()
    */

    val strIndexer = new StringIndexer()
      .setInputCol("boro")
      .setOutputCol("boroid")
      .fit(data)

    // .setInputCols(Array("hour", "zipid", "temp", "precip", "visibility", "wtype", "wind", "ngames"))

    val featAssembler = new VectorAssembler()
      .setInputCols(Array("hour", "boroid", "temp", "precip", "visibility", "wtype", "wind", "snowdepth", "ngames", "max_speed", "min_speed", "avg_speed", "month", "dayofweek"))
      .setOutputCol("features")



    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val vecIndexer = new VectorIndexer()
      .setInputCol("scaledFeatures")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(20)

    /*
    val normalizer = new Normalizer()
      .setInputCol("scaledFeatures")
      .setOutputCol("normFeatures")
    */

    /*
    val rf = new RandomForestRegressor()
      .setFeaturesCol("normFeatures")
      .setLabelCol("count")
      .setNumTrees(100)
    */

    val gbt = new GBTRegressor()
      .setFeaturesCol("indexedFeatures")
      .setLabelCol("count")
      .setMaxIter(20)

    /*
    val lr = new LinearRegression()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("count")
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    */

    val pipeline = new Pipeline()
      .setStages(Array(strIndexer, featAssembler, scaler, vecIndexer, gbt))

    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

    val model = pipeline.fit(train)
    val predictions = model.transform(test)

    predictions.select("prediction", "count", "features").show(5)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("count")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val MAEevaluator = new RegressionEvaluator()
      .setLabelCol("count")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val R2evaluator = new RegressionEvaluator()
      .setLabelCol("count")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val rmse = evaluator.evaluate(predictions)

    val mae = MAEevaluator.evaluate(predictions)

    val r2 = R2evaluator.evaluate(predictions)

    println("RMSE = " + rmse)
    println("MAE = " + mae)
    println("R2 = " + r2)
  }
}
