package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RefreshExample {

  def main(args: Array[String]) = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
    val loadedDF = sparkSession.read.format("csv").option("header", "true").load("../test_data/sales.csv")
    loadedDF.show()

    loadedDF.createOrReplaceTempView("sales")
    // cache table
    sparkSession.catalog.cacheTable("sales")
    println("number of records is "+ sparkSession.table("sales").count)
    // refresh table 
    sparkSession.catalog.refreshTable("sales")
    println("number of records after refresh is " + sparkSession.table("sales").count)
 

  }
}
