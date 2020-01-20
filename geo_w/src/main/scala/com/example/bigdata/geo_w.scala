package com.example.bigdata

import org.apache.spark.SparkConf

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object geo_w {

  def main(args: Array[String]) {
    val path = args(0)

    val conf: SparkConf = new SparkConf().setAppName("geo_w")

    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._

    val geoDataCentral_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataCentral.csv").cache
    val geoDataEastern_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataEastern.csv").cache
    val geoDataMountain_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataMountain.csv").cache
    val geoDataPacific_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataPacific.csv").cache
    val allGeoData_DS = geoDataCentral_DS.union(geoDataEastern_DS).union(geoDataMountain_DS).union(geoDataPacific_DS).dropDuplicates("State")
    val states = allGeoData_DS.drop("Zipcode", "City", "County", "Country")

    val byId = Window.orderBy($"State".asc)
    val statesWithId = states.withColumn("id", row_number over byId)

    val geo_w = statesWithId.select("id", "State", "Timezone")

    spark.sql("use etl_hd")
    geo_w.write.insertInto("geo_w")

  }

}
