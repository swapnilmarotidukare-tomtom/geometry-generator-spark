ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "hello_world_intellij",
    idePackagePrefix := Some("com.tomtom.adeagg")
  )

libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.20.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"

