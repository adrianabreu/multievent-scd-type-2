package com.adrianabreu.scdtype2delta

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class PipelineSpec extends AnyFlatSpec with TestSparkSession with Matchers {

  "The Pipeline object" should "run with a mock input without problems" in {
    import spark.implicits._ 

    val path = new File(this.getClass.getResource("/parades_sample.csv").getPath).getPath

    lazy val reader = (spark: SparkSession) =>
      spark.read
        .format("csv")
        .option("header","true")
        .load(path)

    lazy val expected = Seq[(String, String, String, String)](
     ("680249","2","2.1989850510752147","41.39310425052752"),
     ("674788","3", "2.157891256250179","41.38854563338069"),
     ("689821","4", "2.1587318610871247","41.391021357114504")
    )
    lazy val writer = (df: DataFrame) => {
      val actual = df.as[(String, String, String, String)].collect()
      assert(expected.sortBy(_._1) == actual.sortBy(_._1).toList)
      ()
    }

    val pipeline = new Pipeline(spark, reader, writer)

    pipeline.run()

  }

}
