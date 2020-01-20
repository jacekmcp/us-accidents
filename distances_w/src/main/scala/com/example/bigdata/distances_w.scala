package com.example.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object distances_w {

  def main(args: Array[String]) {
    val path = args(0)

    val conf: SparkConf = new SparkConf().setAppName("distances_w")

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
      dropDuplicates(Array("ID"))

    val uniqeDistances = allMainData_DS.dropDuplicates(Array("Distance(mi)")).select("Distance(mi)");

    def getId = (dystans: Float) => {
      if (dystans == 0) 0
      else if (dystans < 1) 1
      else if (dystans < 10) 2
      else 3
    }

    val getIdUDF = udf(getId)

    val kategorie_DS = Seq((0, "0"), (1, "Ponizej 1 mili"), (2, "Ponizej 10 mil"), (3, "Powyzej 10 mil")).toDF("id_k", "kategoria")

    val distances_DS_temp = uniqeDistances.
      withColumnRenamed("Distance(mi)", "dystans").
      withColumn("id", getIdUDF($"dystans")).
      select("id","dystans")

    val distances_DS = distances_DS_temp.
      join(kategorie_DS, distances_DS_temp("id").equalTo(kategorie_DS("id_k")), "leftouter").
      select("id","dystans", "kategoria")

    val dystanse_wymiar = distances_DS.dropDuplicates(Array("kategoria")).drop($"dystans")

    spark.sql("use etl_hd")
    dystanse_wymiar.write.insertInto("dystans_w")


  }


}
