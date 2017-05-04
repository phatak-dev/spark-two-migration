package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CrossJoin {

  def main(args: Array[String]) = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
    val loadedDf = sparkSession.read.format("csv").option("header", "true").load("../test_data/sales.csv")
    //cross join the data. Fails with normal join
    //val crossJoinDf = loadedDf.join(loadedDf)
    //println(crossJoinDf.count)
 
    val crossJoinDf = loadedDf.crossJoin(loadedDf)
    crossJoinDf.count
  }
}
