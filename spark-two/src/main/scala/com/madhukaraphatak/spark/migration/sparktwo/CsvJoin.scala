package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CsvJoin {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
    // Using 2007.csv from http://stat-computing.org/dataexpo/2009/the-data.html
    val loadedDf = sparkSession.read.format("csv").option("header", "true").load(args(0))
    val miniDf = loadedDf.limit(10000)
    val joinedDf = loadedDf.join(miniDf, loadedDf.col("Year") === miniDf.col("Year"))
    joinedDf.count
  }
}
