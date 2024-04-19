package com.adrianabreu.scdtype2delta

import org.apache.spark.sql.SparkSession

trait TestSparkSession {

  lazy val spark: SparkSession = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName("testing-local")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    session.sparkContext.setLogLevel("WARN")
    session
  }
}