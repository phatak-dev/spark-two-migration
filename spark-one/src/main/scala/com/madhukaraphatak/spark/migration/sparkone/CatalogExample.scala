package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object CatalogExample {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val loadedDf = sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").load("../test_data/sales.csv")

    loadedDf.registerTempTable("sales")
    // show all the tables
    sqlContext.tables.show()
    // just the table names
    println(sqlContext.tableNames().toList)
    // check is table is cached
    println(sqlContext.isCached("sales"))

    // create external table using csv file
    val hiveContext = new HiveContext(sparkContext)
    hiveContext.setConf("hive.metastore.warehouse.dir", "/tmp")
    hiveContext.createExternalTable("sales_external", "com.databricks.spark.csv", Map(
      "path" -> "../test_data/sales.csv",
      "header" -> "true"))
    hiveContext.table("sales_external").show()
  }
}
