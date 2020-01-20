package com.example.bigdata

import org.apache.spark.SparkConf

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf

object fakty_f {

  def main(args: Array[String]) {
    val path = args(0)

    val conf: SparkConf = new SparkConf().setAppName("fakty_f")

    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._

    // DYSTANS
    val mainDataCentral_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataCentral.csv").
      cache();
    val mainDataEastern_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataEastern.csv").
      cache();
    val mainDataMountain_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataMountain.csv").
      cache();
    val mainDataPacific_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(path + "/mainDataPacific.csv").
      cache();
    val allMainData_DS = mainDataCentral_DS.
      union(mainDataEastern_DS).
      union(mainDataMountain_DS).
      union(mainDataPacific_DS).
      dropDuplicates(Array("ID"));

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


    ////////////////////////////POGODA///////////////////////////
    val weather_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(path + "/weather.txt").cache();

    val regexWeather = """(?<=Weather Condition: ).+""".r
    val regexTimestamp="""[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]""".r
    val regexCode = """(?<=airport )[A-Z0-9]{4}""".r

    val GetWeather: String => String = regexWeather.findFirstIn(_).get
    val GetDate: String => String = regexTimestamp.findFirstIn(_).get
    val GetCode: String => String = regexCode.findFirstIn(_).get

    val getWeatherUDF = udf(GetWeather)
    val getDateUDF = udf(GetDate)
    val getCodeUDF = udf(GetCode)


    val weather_data = weather_DS.withColumnRenamed("_c8", "weather").
      withColumnRenamed("_c0", "dateCol").
      withColumn("date", getDateUDF($"dateCol").cast("timestamp")).
      withColumn("Code", getCodeUDF($"dateCol")).
      withColumn("weather", getWeatherUDF($"weather")).
      select("weather", "date", "Code")

    val weatherTab_DS = weather_DS.withColumnRenamed("_c8", "weather").
      withColumn("weather", getWeatherUDF($"weather")).
      select("weather").dropDuplicates("weather")

    val w = Window.orderBy("weather")
    val weather_with_id = weatherTab_DS.withColumn("id", row_number().over(w))
    val weather_wymiar = weather_with_id.withColumnRenamed("weather", "nazwa").select("nazwa", "id")

    //////////////////////////GEO/////////////////////////////

    val geoDataCentral_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataCentral.csv").cache
    val geoDataEastern_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataEastern.csv").cache
    val geoDataMountain_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataMountain.csv").cache
    val geoDataPacific_DS = spark.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv(path + "/geoDataPacific.csv").cache
    val allGeoData_DS = geoDataCentral_DS
     .union(geoDataEastern_DS).union(geoDataMountain_DS).union(geoDataPacific_DS)

    ////////////////////////FACTS DATA//////////////////////////


    val main_Central_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      option("multiline", value= true).
      csv(path + "/mainDataCentral.csv").cache().select(
      substring($"Start_Time", 0, 10).as("Start_Date"),
      $"Start_Time",
      $"End_Time",
      $"Severity",
      $"Distance(mi)",
      $"Zipcode",
      $"Airport_Code"
    )

    main_Central_DS.show()

    val main_Eastern_DS =  spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      option("multiline", value= true).
      csv(path + "/mainDataEastern.csv").cache().select(
      substring($"Start_Time", 0, 10).as("Start_Date"),
      $"Start_Time",
      $"End_Time",
      $"Severity",
      $"Distance(mi)",
      $"Zipcode",
      $"Airport_Code"
    )

    val main_Mountain_DS =  spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      option("multiline", value= true).
      csv(path + "/mainDataMountain.csv").cache().select(
      substring($"Start_Time", 0, 10).as("Start_Date"),
      $"Start_Time",
      $"End_Time",
      $"Severity",
      $"Distance(mi)",
      $"Zipcode",
      $"Airport_Code"
    )

    val main_Pacific_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      option("multiline", value= true).
      csv(path + "/mainDataPacific.csv").cache().select(
      substring($"Start_Time", 0, 10).as("Start_Date"),
      $"Start_Time",
      $"End_Time",
      $"Severity",
      $"Distance(mi)",
      $"Zipcode",
      $"Airport_Code"
    )

    val main_All_DS=main_Central_DS
     .union(main_Eastern_DS).union(main_Mountain_DS).union(main_Pacific_DS)


    ////////////////////////////////TEMPORARY_TABLES/////////////////////////

    val pom_dist_DS = distances_DS.withColumnRenamed("id", "id_dist").select("id_dist", "dystans", "kategoria");

    val pom_geo_DS = allGeoData_DS.withColumnRenamed("Zipcode", "geoZipcode").select("geoZipcode", "State")

    val pom_weather_wymiar = weather_wymiar.withColumnRenamed("id", "id_weather").withColumnRenamed("nazwa", "nazwa_pogody").select("id_weather", "nazwa_pogody")

    val pom_weather_DS = weather_data.
      withColumnRenamed("Code", "w_code").
      withColumnRenamed("date", "w_date").
      join(pom_weather_wymiar, $"weather" === $"nazwa_pogody").
      select("w_code", "w_date", "weather", "id_weather");

    /////////////////////////CREATE FACTS//////////////////////

    val all_Facts = main_All_DS.
      join(pom_dist_DS, $"Distance(mi)" === $"dystans").
      join(pom_geo_DS, $"Zipcode" === $"geoZipcode").
      join(pom_weather_DS, unix_timestamp($"w_date")>=unix_timestamp($"Start_Time")-3600 && unix_timestamp($"w_date")<=unix_timestamp($"End_Time")+3600 && $"w_code" === $"Airport_Code", "LeftOuter").
      dropDuplicates("Start_Time").
      select(
        $"Start_Date".as("Date"),
        $"Start_Time",
        $"End_Time",
        $"Severity",
        $"Distance(mi)".as("Distance"),
        $"id_dist".as("Distance_Category"),
        $"id_weather",
        $"State").
      withColumn("Length_in_sec", (unix_timestamp($"End_Time") - unix_timestamp($"Start_Time"))).
      groupBy($"Date",
        $"State",
        $"Severity",
        $"Distance_Category"
        ,$"id_weather"
      ).
      agg(count($"Severity").as("Accidents_Count"),
        sum("Distance").as("sum_Distance"),
        sum("Length_in_sec").as("sum_Duration_Time")
      ).drop("Start_Time",
      "geoZipcode",
      "Zipcode",
      "End_Time",
      "Distance",
      "Length_in_sec",
      "Airport_Code"
    )

    all_Facts.show()
    spark.sql("use etl_hd")
    all_Facts.write.insertInto("fakty_f")

  }

}
