ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "com.example"

lazy val sparkVersion = "3.5.9"

lazy val root = (project in file("."))
  .settings(
    name := "telemetry-legacy-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
