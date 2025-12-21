ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / evictionErrorLevel := Level.Warn
ThisBuild / libraryDependencySchemes +=
  "org.scala-lang.modules" %% "scala-parser-combinators" % "early-semver"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-proccess",
    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql"  % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.spark" %% "spark-mllib" % "3.5.0",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
      "com.typesafe.akka" %% "akka-http"   % "10.2.10",
      "com.typesafe.akka" %% "akka-stream" % "2.6.20",
      "com.typesafe.akka" %% "akka-actor"  % "2.6.20",
      "io.spray" %% "spray-json" % "1.3.6",
      "org.apache.pdfbox" % "pdfbox" % "2.0.30",
      "org.apache.poi" % "poi" % "5.2.5",
      "org.apache.poi" % "poi-ooxml" % "5.2.5",
      "org.apache.poi" % "poi-scratchpad" % "5.2.5"
    ),
    dependencyOverrides ++= Seq(
      "com.typesafe.akka" %% "akka-actor"  % "2.6.20",
      "com.typesafe.akka" %% "akka-stream" % "2.6.20"
    )
  )
