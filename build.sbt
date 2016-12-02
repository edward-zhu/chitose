name := "Project chitose"

version := "1.0"

scalaVersion := "2.11.8"

aggregate in update := false

updateOptions := updateOptions.value.withCachedResolution(true)

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0",
  "ml.dmlc" % "xgboost4j" % "0.7",
  "ml.dmlc" % "xgboost4j-spark" % "0.7",
  "org.joda" % "joda-convert" % "1.2"
)