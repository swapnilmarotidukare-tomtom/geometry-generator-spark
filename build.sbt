ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "hello_world_intellij",
    idePackagePrefix := Some("com.tomtom.adeagg")
  )

val sparkVersion = "3.5.2"
val hadoopVersion = "3.3.6"

libraryDependencies ++= Seq(
  "org.locationtech.jts" % "jts-core" % "1.20.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "io.delta" %% "delta-spark" % "3.1.0",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "io.netty" % "netty-all" % "4.1.70.Final",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
//  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "com.google.guava" % "guava" % "31.1-jre"
)



