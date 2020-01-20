package com.example.bigdata

import java.time.LocalDate

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLImplicits

object fakty_f {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkWordCount")

    val sc: SparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val mainDataCentral_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/mainDataCentral.csv").cache
    val mainDataEastern_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/mainDataEastern.csv").cache
    val mainDataMountain_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/mainDataMountain.csv").cache
    val mainDataPacific_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/mainDataPacific.csv").cache
    val allMainData_DS = mainDataCentral_DS.union(mainDataEastern_DS).union(mainDataMountain_DS).union(mainDataPacific_DS).dropDuplicates(Array("ID"))
    val uniqeDistances = allMainData_DS.dropDuplicates(Array("Distance(mi)")).select("Distance(mi)")
    val byId = Window.orderBy("Distance(mi)")
    val distancesWithId = uniqeDistances.withColumn("id", row_number().over(byId))
    val distancesWithIdSwaped = distancesWithId.withColumnRenamed("id", "id").
      withColumnRenamed("Distance(mi)", "dystans").
      select("id","dystans")



    val geoDataCentral_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/geoDataCentral.csv").cache
    val geoDataEastern_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/geoDataEastern.csv").cache
    val geoDataMountain_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/geoDataMountain.csv").cache
    val geoDataPacific_DS = sqlContext.read.format("org.apache.spark.csv").option("header", true).option("inferSchema", true).csv("/home/jacek/Downloads/school/Big Data/us-accidents/sampleData/geoDataPacific.csv").cache
    val allGeoData_DS = geoDataCentral_DS.union(geoDataEastern_DS).union(geoDataMountain_DS).union(geoDataPacific_DS).dropDuplicates("State")
    val states = allGeoData_DS.drop("Zipcode", "City", "County", "Country")

    val byID = Window.orderBy($"State".asc)
    val statesWithId = states.withColumn("id", row_number over byID)

    val geo_DS = statesWithId.select("id", "State", "Timezone")

    val uniqeStartTimes = allMainData_DS.dropDuplicates(Array("Start_Time")).select("Start_Time")

    val getWeekDay: String => String = LocalDate.parse(_, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getDayOfWeek.name()
    val getYear: String => Integer = _.substring(0,4).toInt
    val getMonth: String => Integer = _.substring(5,7).toInt

    val getYearUDF = udf(getYear)
    val getMonthUDF = udf(getMonth)
    val getWeekDayUDF = udf(getWeekDay)

    val daty_DS = uniqeStartTimes.
      withColumn("rok", getYearUDF($"Start_Time")).
      withColumn("miesiac", getMonthUDF($"Start_Time")).
      withColumn("data", $"Start_Time".cast("date")).
      withColumn("dzien_tygodnia", getWeekDayUDF($"Start_Time")).
      drop($"Start_Time")


    val distances_DF = distancesWithIdSwaped.as("dys")
    val dates_DF = daty_DS.as("dat")
    val geo_DF = geo_DS.as("geo")
//    val pogoda_DF = pogoda_DS.as("pog")


    val allGeoDataWithZipcode_DS = geoDataCentral_DS.union(geoDataEastern_DS).union(geoDataMountain_DS).union(geoDataPacific_DS)

//    allGeoDataWithZipcode_DS.show()
//    allMainData_DS.show()
    val mainDataWithGeoState = allMainData_DS.join(allGeoDataWithZipcode_DS.
      select("Zipcode","State"), allGeoDataWithZipcode_DS("Zipcode") === allMainData_DS("Zipcode")).
      drop("Zipcode")

//    mainDataWithGeoState.show()


  //To jest chuj to nie powinno być joinów
    //Tabela faktów
//    val accidentsFacts = mainDataWithGeoState.select("Start_Time", "Zipcode", "Distance(mi)").
//      join(dates_DF, allMainData_DS("Start_Time") === dates_DF("dat.data")).
//      join(geo_DF, mainDataWithGeoState("State") === geo_DF("geo.state")).
//      join(distances_DF, allMainData_DS("Distance(mi)") === distances_DF("dys.dystans")).
////      join(pogoda_DF, allMainData_DS("Start_Time") === pogoda_DF("pog.date")).
//      groupBy("Start_Time", "Distance(mi)", "Zipcode").
//      agg(sum($"Distance(mi)").as("suma_dyst"), count("ID").as("liczba_zdarzen")).
//      withColumnRenamed("Start_Time", "date").
//      withColumnRenamed("Zipcode", "zip_code").
//      withColumnRenamed("dys.id", "id_dyst").
//      withColumnRenamed("pog.id", "id_pogody").
//      select("date", "zip_code", "id_dyst", "id_pogody", "severity", "liczba_zdarzen", "suma_dyst")


    val accidentsFacts = mainDataWithGeoState.
      withColumn("Severity", $"Severity".cast("integer")).
      withColumn("xd", ($"End_Time".cast("timestamp").cast("integer") - $"Start_Time".cast("timestamp").cast("integer")) / 60).
      withColumn("dystans", $"Distance(mi)").
      withColumn("State", $"State")
//      withColumn("czas_trwania", ($"End_Time".cast("integer") - $"Start_Time".cast("integer")).cast())


    accidentsFacts.show()
  }


}
