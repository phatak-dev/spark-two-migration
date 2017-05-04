package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CsvJoin {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    // Using 2007.csv from http://stat-computing.org/dataexpo/2009/the-data.html
    val loadedDf = sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").load(args(0))

    val miniDf = loadedDf.limit(10000)

    val joinedDf = loadedDf.join(miniDf, loadedDf.col("Year") === miniDf.col("Year"))

    println(joinedDf.count)

    sparkContext.stop()
  }
}
