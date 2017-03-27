package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CsvLoad {

  def main(args:Array[String]) = {

     val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
     val sparkContext = new SparkContext(sparkConf)
     val sqlContext = SQLContext.getOrCreate(sparkContext)

     val loadedDF = sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("../test_data/sales.csv")
     
     loadedDF.show()

  }
}
