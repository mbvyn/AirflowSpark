package com.tvshow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * The simple Spark application that reads the data from a CSV file.
 *
 * The following code performs an analysis on TV show data.
 *
 * It takes the following arguments:
 * application name,
 * log level,
 * Input file path
 *
 * And returns the analysis result to console
 * */
object TvShowAnalysis extends App {
  require(args.length == 3, "Usage: TvShowAnalysis <app name> <log level> <input-file>")

  val appName = args(0)
  val logLevel = args(1)
  val inputPath = args(2)

  val spark = SparkSession.builder
    .appName(appName)
    .getOrCreate()

  spark.sparkContext.setLogLevel(logLevel)

  val tvShowsDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputPath)

  val groupedTvShowsDF = tvShowsDF
    .filter(col("rating").isNotNull)
    .groupBy("type", "rating")
    .count()
    .orderBy(desc("count"))
    .limit(20)

  groupedTvShowsDF.show(false)
}