package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object RefreshExample {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val hiveContext = new HiveContext(sparkContext)


    val loadedDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../test_data/sales.csv")
    loadedDF.registerTempTable("sales")
    // cache table
    hiveContext.cacheTable("sales")
    println("number of records is "+ hiveContext.table("sales").count)
    // refresh table 
    hiveContext.refreshTable("sales")
    println("number of records after refresh is " + hiveContext.table("sales").count)
  }
}
