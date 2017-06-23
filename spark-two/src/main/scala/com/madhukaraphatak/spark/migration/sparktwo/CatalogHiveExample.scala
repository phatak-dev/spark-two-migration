package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CatalogHiveExample {

  def main(args: Array[String]) = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").
        enableHiveSupport().getOrCreate()
    // show all the tables
    sparkSession.catalog.listTables.show()
    //read table from hive
    val df = sparkSession.table("sales")
    df.show()
    // save table to hive
    df.write.saveAsTable("sales_saved")
  }
}
