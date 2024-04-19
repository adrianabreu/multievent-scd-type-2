package com.adrianabreu.scdtype2delta
import com.adrianabreu.scdtype2delta.model.schemas.Stops
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, regexp_extract}
import org.slf4j.LoggerFactory

class Pipeline(spark: SparkSession, reader: SparkSession => DataFrame, writer: DataFrame => Unit) {
    private val log = LoggerFactory.getLogger(classOf[Pipeline])

    def run(): Unit = {
        log.info("Start etl process")
        val data = reader(spark)

        val transformed = Transformations.extractGeoInformation(data)

        log.info("Writing data")
        writer(transformed)

        log.info("Finished")
    }
}


object Transformations {
    def extractGeoInformation(df: DataFrame): DataFrame = {

        val regexp = """POINT \(([\d\.]*) ([\d\.]*)\)"""
        df.select(
            col(Stops.id).alias("id"),
            col(Stops.code).alias("code"),
            regexp_extract(col(Stops.geometry),regexp,1).alias("x"),
            regexp_extract(col(Stops.geometry),regexp,2).alias("y")
        )
    }
}