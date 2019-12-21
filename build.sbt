name := "mapreduce"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview"
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview",
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview")

