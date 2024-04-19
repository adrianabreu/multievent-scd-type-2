import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.adrianabreu"


val projectName = "scd-type-2-delta"

lazy val root = (project in file("."))
  .settings(
    name := projectName,
    assemblyJarName := s"${projectName}-latest.jar",
    libraryDependencies ++= Seq(
        Spark.core % Provided,
        Spark.sql  % Provided,
        delta      % Provided,
        log4j      % Compile,
        scalaTest  % Test
    ),
    parallelExecution in Test := false
  )