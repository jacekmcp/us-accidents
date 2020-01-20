package com.example.bigdata

import java.time.LocalDate

import org.apache.spark.SparkConf
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object date_w {

  def main(args: Array[String]) {
    val path = args(0)

    val conf: SparkConf = new SparkConf().setAppName("date_w")

    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._

    val mainDataCentral_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataCentral.csv").
      cache()
    val mainDataEastern_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataEastern.csv").
      cache()
    val mainDataMountain_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataMountain.csv").
      cache()
    val mainDataPacific_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataPacific.csv").
      cache()
    val allMainData_DS = mainDataCentral_DS.
      union(mainDataEastern_DS).
      union(mainDataMountain_DS).
      union(mainDataPacific_DS).
      dropDuplicates(Array("ID"));

    val uniqeStartTimes = allMainData_DS.dropDuplicates(Array("Start_Time")).select("Start_Time");

    val getWeekDay: String => String = LocalDate.parse(_, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getDayOfWeek.name()

    val getWeekDayUDF = udf(getWeekDay)

    val daty_DS = uniqeStartTimes.
      withColumn("data", $"Start_Time".cast("date")).
      withColumn("dzien_tygodnia", getWeekDayUDF($"Start_Time")).
      dropDuplicates(Array("data")).
      drop($"Start_Time")

    spark.sql("use etl_hd")
    daty_DS.write.insertInto("date_w")

  }

}
