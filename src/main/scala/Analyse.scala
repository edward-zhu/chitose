import ml.dmlc.xgboost4j.scala.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DoubleType
import utils.PathFinder

/**
  * Created by edwardzhu on 2016/11/21.
  */
object Analyse {
  def preprocess(raw : DataFrame): DataFrame = {
    // convert all target value to double type
    val data = raw
      .withColumn("count", raw.col("count").cast(DoubleType))
      .withColumn("kill", raw.col("kill").cast(DoubleType))
      .withColumn("wound", raw.col("wound").cast(DoubleType))

    // convert string value to id in integer type
    val strIndexer = new StringIndexer()
      .setInputCol("boro")
      .setOutputCol("boroid")
      .fit(data)

    // feature select and combine
    val featAssembler = new VectorAssembler()
      .setInputCols(Array("hour", "boroid",
        "temp", "precip", "visibility", "wtype", "wind", "snowdepth",
        "ngames",
        "max_speed", "min_speed", "avg_speed",
        "month", "dayofweek"))
      .setOutputCol("features")

    // normalize feature to deviation to 1
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    // .setWithMean(true) // set to true to make mean be 0

    // automatically convert some feature to category type
    val vecIndexer = new VectorIndexer()
      .setInputCol("scaledFeatures")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(20)

    val pipeline = new Pipeline()
      .setStages(Array(strIndexer, featAssembler, scaler, vecIndexer))

    val model = pipeline.fit(data)

    val preprocessed = model.transform(data)

    preprocessed
      .select("indexedFeatures", "count")
      .withColumnRenamed("indexedFeatures", "features")
  }

  def evaluate(predictions: DataFrame): Unit = {
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

  def main(args: Array[String]): Unit = {
    // Simplify Log printing
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sc = SparkContext.getOrCreate();
    val sql = SparkSession.builder().getOrCreate();

    // Get data file
    val raw = sql.read.parquet(PathFinder.getPath("total.parquet"))


    val total = preprocess(raw)

    // Random Forest Regression Model

    val rf = new RandomForestRegressor()
      .setFeaturesCol("indexedFeatures")
      .setLabelCol("count")
      .setNumTrees(100)


    // Gradient-Boosted Tree Regression Model

    val gbt = new GBTRegressor()
      .setFeaturesCol("indexedFeatures")
      .setLabelCol("count")
      .setMaxIter(20)

    val paramMap = List(
      "num_round" -> 100,
      "eval_metric" -> "rmse",
      "objective" -> "count:poisson").toMap

    val xgb = new XGBoostEstimator(paramMap)
      .setFeaturesCol("features")
      .setLabelCol("count")


    val Array(train, test) = total.randomSplit(Array(0.8, 0.2))

    val model = xgb.train(train)

    val predictions = model.transform(test)

    evaluate(predictions)

  }
}
