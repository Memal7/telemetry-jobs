ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "com.example"

lazy val sparkVersion = "3.5.9"

lazy val root = (project in file("."))
  .settings(
    name := "telemetry-legacy-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    // Spark on JDK 17.
    Test / javaOptions ++= Seq(
      "java.base/java.lang",
      "java.base/java.lang.invoke",
      "java.base/java.nio",
      "java.base/java.util",
      "java.base/sun.nio.ch",
      "java.base/sun.util.calendar"
    ).map(pkg => s"--add-opens=$pkg=ALL-UNNAMED"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
