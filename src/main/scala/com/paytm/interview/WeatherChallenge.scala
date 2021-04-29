package com.paytm.interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lag, substring, sum, when}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.storage.StorageLevel

/**
 * Object to process weather data from 2019 and do relevant processing
 *
 * @author prakarshupmanyu<prakarsh.upmanyu23@gmail.com>
 */
object WeatherChallenge extends App {

  //Change this directory accordingly before running the code
  val ROOT_DIR = "/Users/prakarsh/Desktop/paytmteam-de-weather-challenge-beb4fc53605c"

  val spark = SparkSession.builder
    .appName("CodingChallenge")
    .getOrCreate()

  //STEP 1 - Part 1 - Loading global weather data
  val weatherDataDF = spark.read.format("csv")
    .option("header", "true")
    .load(ROOT_DIR + "/data/2019/*.csv.gz")

  //STEP 1 - Part 2 - Join stationlist.csv with countrylist.csv
  val countryDF = spark.read.format("csv")
    .option("header", "true")
    .load(ROOT_DIR + "/countrylist.csv")

  val stationDF = spark.read.format("csv")
    .option("header", "true")
    .load(ROOT_DIR + "/stationlist.csv")

  /*
    Doing a left join because there exists few entries for COUNTRY_ABBR in stationlist.csv
    which do not exist in countrylist.csv, for example AE for STN_NO = 412161.
    For the COUNTRY_FULL value for such missing entries, using the COUNTRY_ABBR value from stationlist.csv
    At the end, remove the abbreviation column
  */
  val countryStationDF = stationDF
    .join(countryDF, stationDF("COUNTRY_ABBR") === countryDF("COUNTRY_ABBR"), "left")
    .drop(countryDF("COUNTRY_ABBR"))
    .withColumn("COUNTRY_FULL",
      when(col("COUNTRY_FULL").isNull, col("COUNTRY_ABBR"))
        .otherwise(col("COUNTRY_FULL")))
    .select("STN_NO", "COUNTRY_FULL")

  //STEP 1 - Part 3 - Joining the weather data to include the full country names. Join is carried out on STN_NO and STN---
  val mergedGlobalWeatherDataDF = weatherDataDF
    .join(countryStationDF, weatherDataDF("`STN---`") === countryStationDF("STN_NO"))

  /*
    Cleaning the joined data by changing types and removing columns that are not required for this
    challenge to reduce the size so that it is easier to cache in memory (disk caching also added, just in case)
   */
  val cleanGlobalWeatherDF = mergedGlobalWeatherDataDF
    .drop(countryStationDF("STN_NO"))
    .drop("SNDP")
    .drop("PRCP")
    .drop("MIN")
    .drop("MAX")
    .drop("GUST")
    .drop("MXSPD")
    .drop("VISIB")
    .drop("STP")
    .drop("SLP")
    .drop("DEWP")
    .drop("WBAN")
    .withColumn("YEARMODA", col("YEARMODA").cast(IntegerType))
    .withColumn("TEMP", col("TEMP").cast(FloatType))
    .withColumn("WDSP", col("WDSP").cast(FloatType))
    .persist(StorageLevel.MEMORY_AND_DISK)

  //STEP 2 - Question 1 - Which country had the hottest average mean temperature over the year?

  /*
    We only need to pay around with TEMP and COUNTRY_FULL
    I decided to filter out the rows which indicate missing TEMP values instead of assigning them 0
    It is better to compute average for data that we have. If we replace missing values with 0,
    we would end up increasing the denominator for the average i.e. count, and that would lower the average
    Grouping by COUNTRY_FULL and computing average for each group/country and picking the country with
    the highest average
   */
  val hottestCountryByAvgTemp = cleanGlobalWeatherDF
    .select("COUNTRY_FULL", "TEMP")
    .filter("TEMP < 9999.9")
    .groupBy("COUNTRY_FULL").agg(avg("TEMP").as("AVG_TEMP"))
    .sort(col("AVG_TEMP").desc)
    .first
    .getString(0)

  println(s"Which country had the hottest average mean temperature over the year :: $hottestCountryByAvgTemp")

  //STEP 2 - Question 2 - Which country had the most consecutive days of tornadoes/funnel cloud formations?

  /*
    We only need to play around with COUNTRY_FULL, YEARMODA and FRSHTT here.
    The logic used is to first find if there was a tornado or funnel sighting on a specific day in a country
    So, I reduced FRSHTT to the last T. Then we are only interested in the rows where the T or TORNADO value is 1.
    Distinct is used because duplicate values for Country and tornado date doesn't help us.
   */
  val countryDateTornadoFunnelDF = cleanGlobalWeatherDF
    .select("COUNTRY_FULL", "YEARMODA", "FRSHTT")
    .withColumn("TORNADO", substring(col("FRSHTT"), 6, 6).cast(IntegerType))
    .drop("FRSHTT")
    .filter("TORNADO = 1")
    .drop("TORNADO")
    .distinct

  /*
    Now we want to find the number of consecutive days with tornado sightings so for every country we sort by the date.
    Then we find out against each sighting, when was the last time a tornado was sighted in this country/partition. This
    is accomplished by lag() function over a SORTED by date partition.
    Then we can compute the number of days it has been since the last tornado in that country/partition
    We only case about consecutive days so we ignore the rows where it has been more than 1 day since the last tornado sighting
    Lastly, we compute the grouped sum for each country, sort it and pick the country with maximum sum
   */
  val win = Window.partitionBy("COUNTRY_FULL").orderBy("YEARMODA")

  val countryWithMostConsecutiveTornadoes = countryDateTornadoFunnelDF
    .withColumn("PREVIOUS_TORNADO_DAY", lag("YEARMODA", 1, 0).over(win))
    .withColumn("NUM_DAYS_SINCE_LAST_TORNADO", col("YEARMODA") - col("PREVIOUS_TORNADO_DAY"))
    .withColumn("NUM_DAYS_SINCE_LAST_TORNADO", when(col("NUM_DAYS_SINCE_LAST_TORNADO") > 1, 0).otherwise(col("NUM_DAYS_SINCE_LAST_TORNADO")))
    .drop("PREVIOUS_TORNADO_DAY")
    .groupBy("COUNTRY_FULL")
    .agg(sum("NUM_DAYS_SINCE_LAST_TORNADO").as("TOTAL_CONSECUTIVE_DAYS_TORNADO"))
    .sort(col("TOTAL_CONSECUTIVE_DAYS_TORNADO").desc)
    .first
    .getString(0)

  println(s"Which country had the most consecutive days of tornadoes/funnel cloud formations :: $countryWithMostConsecutiveTornadoes")

  //STEP 2 - Question 3 - Which country had the second highest average mean wind speed over the year?

  /*
    We only need to pay around with WDSP and COUNTRY_FULL
    I decided to filter out the rows which indicate missing WDSP values instead of assigning them 0
    It is better to compute average for data that we have. If we replace missing values with 0,
    we would end up increasing the denominator for the average i.e. count, and that would lower the average
    Grouping by COUNTRY_FULL and computing average for each group/country and picking the country with
    the SECOND highest average
   */
  val secondHighestCountryByAvgWindSpeed = cleanGlobalWeatherDF
    .select("COUNTRY_FULL", "WDSP")
    .filter("WDSP < 999.9")
    .groupBy("COUNTRY_FULL").agg(avg("WDSP").as("AVG_WDSP"))
    .sort(col("AVG_WDSP").desc)
    .take(2)(1)
    .getString(0)

  println(s"Which country had the second highest average mean wind speed over the year :: $secondHighestCountryByAvgWindSpeed")
}
