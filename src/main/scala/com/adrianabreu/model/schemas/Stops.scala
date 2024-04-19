package com.adrianabreu.scdtype2delta.model.schemas
import org.apache.spark.sql.types._

object Stops {

    val fid = "FID"
    val id = "ID_PARADA"
    val code = "CODI_PARADA"
    val name = "NOM_PARADA"
    val description = "DESC_PARADA"
    val hubCode = "CODI_INTERC"
    val hubName = "NOM_INTERC"
    val typeName = "NOM_TIPUS_PARADA"
    val typeSimpleName = "NOM_TIPUS_SIMPLE_PARADA"
    val typeDescription = "DESC_TIPUS_PARADA"
    val categorization = "TIPIFICACIO_PARADA"
    val address = "ADRECA"
    val cityId = "ID_POBLACIO"
    val cityName = "NOM_POBLACIO"
    val districtId = "ID_DISTRICTE"
    val districtName = "NOM_DISTRICTE"
    val date = "DATA"
    val streeName = "NOM_VIA"
    val nextStreeName = "NOM_PROPERA_VIA"
    val geometry = "GEOMETRY"
    val stopDots = "PUNTS_PARADA"

    def schema: StructType = new StructType(
        Seq(
            StructField(fid,StringType),
            StructField(id,IntegerType),
            StructField(code,IntegerType),
            StructField(name,StringType),
            StructField(description,StringType),
            StructField(hubCode,IntegerType),
            StructField(hubName,StringType),
            StructField(typeName,StringType),
            StructField(typeSimpleName,StringType),
            StructField(typeDescription,StringType),
            StructField(categorization,StringType),
            StructField(address,StringType),
            StructField(cityId,StringType),
            StructField(cityName,StringType),
            StructField(districtId,StringType),
            StructField(districtName,StringType),
            StructField(date,StringType),
            StructField(streeName,StringType),
            StructField(nextStreeName,StringType),
            StructField(geometry,StringType),
            StructField(stopDots,StringType)
        ).toArray
    )
}

