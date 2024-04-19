package com.adrianabreu.scdtype2delta

import com.adrianabreu.scdtype2delta.args.ArgParser
import com.adrianabreu.scdtype2delta.model.schemas.Stops
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger

object Main {
  def main(args: Array[String]): Unit = {

    val parsedArgs = ArgParser.parse(args)

    val log            = Logger.getRootLogger
    val spark = buildSparkSession(parsedArgs("env").toString)

    lazy val reader = (spark: SparkSession) =>
      spark.read
        .format("csv")
        .option("header","true")
        .schema(Stops.schema)
        .load(parsedArgs("filename").toString)

    lazy val writer = (df: DataFrame) =>
      df.coalesce(1)
        .write
        .mode("append")
        .format("delta")
        .save("stops")

    val pipeline = new Pipeline(spark, reader, writer)

    pipeline.run()
  }

  def buildSparkSession(env: String): SparkSession = {
    val sessionBuilder = SparkSession.builder.config(
      "spark.sql.session.timeZone",
      "UTC"
    )

    if (env == "dev") 
      sessionBuilder.master("local")
        .appName("local session")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    sessionBuilder.getOrCreate()
  }
}