name := "Project chitose"

version := "1.0"

scalaVersion := "2.10.5"

aggregate in update := false

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "org.joda" % "joda-convert" % "1.2"
)