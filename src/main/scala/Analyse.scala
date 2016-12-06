import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import utils.PathFinder

/**
  * Created by edwardzhu on 2016/11/21.
  */
object Analyse {
  val featGroup = Map(
    "basic" -> List("hour", "boroid"),
    "weather" -> List("temp", "precip", "visibility", "wtype", "wind", "snowdepth"),
    "games" -> List("ngames"),
    "speed" -> List("max_speed", "min_speed", "avg_speed"),
    "more_time" -> List("month", "dayofweek")
  )

  def preprocess(raw : DataFrame, feats : Array[String]): DataFrame = {
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
      .setInputCols(feats)
      .setOutputCol("features")

    // normalize feature to deviation to 1
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    // .setWithMean(true) // set to true to make mean be 0

    val minMaxScaler = new MinMaxScaler()
      .setInputCol("scaledFeatures")
      .setOutputCol("scaledFeatures2")

    // automatically convert some feature to category type
    val vecIndexer = new VectorIndexer()
      .setInputCol("scaledFeatures2")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(20)


    val pipeline = new Pipeline()
      .setStages(Array(strIndexer, featAssembler, scaler, minMaxScaler, vecIndexer))

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

    if (args.length < 2) {
      println("usage PROG [features] [alg.]");
      System.exit(0);
    }

    val sc = SparkContext.getOrCreate();
    val sql = new HiveContext(sc)

    // Get data file
    val dtrain = sql.read.parquet(PathFinder.getPath("train.parquet"))
    val dtest = sql.read.parquet(PathFinder.getPath("test.parquet"))

    val feats = args(0).split(",")
    val alg = args(1)

    val features = feats.flatMap((feat) => {featGroup(feat)})

    println("selected features : ")
    features.foreach(println)

    val Array(train, test) = Array(preprocess(dtrain, features), preprocess(dtest, features))


    // Random Forest Regression Model

    val rf = new RandomForestRegressor()
      .setLabelCol("count")
      .setNumTrees(100)

    // Gradient-Boosted Tree Regression Model

    val gbt = new GBTRegressor()
      .setLabelCol("count")
      .setMaxIter(20)

    // Linear Regression Model

    val lr = new LinearRegression()
      .setLabelCol("count")
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val algs = Map(
      "rf" -> rf,
      "gbt" -> gbt,
      "lr" -> lr
    )

    val model = algs(alg).fit(train)

    // model.save(PathFinder.getPath("latest.model"))

    val predictions = model.transform(test)

    evaluate(predictions);
  }
}
