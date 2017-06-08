package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CatalogExample {

  def main(args: Array[String]) = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
    val loadedDf = sparkSession.read.format("csv").option("header", "true").load("../test_data/sales.csv")

    loadedDf.createOrReplaceTempView("sales")
    // show all the tables
    sparkSession.catalog.listTables.show()
    // just the table names
    println(sparkSession.catalog.listTables.select("name").collect.toList)
    // check is table is cached
    println(sparkSession.catalog.isCached("sales"))
    //external table creation
    sparkSession.catalog.createExternalTable("sales_external", "com.databricks.spark.csv", Map(
      "path" -> "../test_data/sales.csv",
      "header" -> "true"))
    sparkSession.table("sales_external").show()


    //additional functionalities
    
    //list functions
    sparkSession.catalog.listFunctions.show()
    //list columns of a table
    sparkSession.catalog.listColumns("sales").show()
  }
}
