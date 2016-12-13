import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import utils.PathFinder

import scala.collection.generic.SeqFactory

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

  def evaluate(predictions: DataFrame): List[Double] = {
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

    List(rmse, mae, r2)
  }

  def getImportance(data: DataFrame): Unit = {
    val rf = new RandomForestRegressor()
      .setLabelCol("count")
      .setNumTrees(100)

    val model = rf.fit(data);
    val importance = model.featureImportances;

    println(importance);
  }

  def _train(alg: String, train : DataFrame, test: DataFrame) : Transformer = {
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

    algs(alg).fit(train)
  }

  def train(alg:String, data: DataFrame) : Unit = {
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))
    val model = _train(alg, train, test);
    val predictions = model.transform(test)
    val List(rmse, mae, r2) = evaluate(predictions)

    println("RMSE = " + rmse)
    println("MAE = " + mae)
    println("R2 = " + r2)
  }

  def trainCV(alg:String, data: DataFrame): Unit = {
    val splits = data.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2))

    var Array(rmse, mae, r2) = Array(0.0d, 0.0d, 0.0d)

    for (i <- 0 to 4) {
      println("Round " + i)
      val train = splits.drop(i).reduce(_.unionAll(_))
      val test = splits(i)

      val model = _train(alg, train, test);

      // model.save(PathFinder.getPath("latest.model"))

      val predictions = model.transform(test)

      val List(rmse1, mae1, r21) = evaluate(predictions)

      rmse += rmse1 / 5.0d
      mae += mae1 / 5.0d
      r2 += r2 / 5.0d
    }

    println("RMSE = " + rmse)
    println("MAE = " + mae)
    println("R2 = " + r2)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    if (args.length < 2) {
      println("usage PROG [features] [alg.]");
      System.exit(0);
    }

    val sc = SparkContext.getOrCreate();
    val sql = new HiveContext(sc)

    val feats = args(0).split(",")
    val alg = args(1)

    if (args.length > 2) {
      PathFinder.setCustomPath(args(2))
    }

    // Get data file
    val raw = sql.read.parquet(PathFinder.getPath("total.parquet"))

    val features = feats.flatMap((feat) => {featGroup(feat)})

    println("selected features : ")
    features.foreach(println)

    val data = preprocess(raw, features)

    train(alg, data);

    // getImportance(data);
  }
}
