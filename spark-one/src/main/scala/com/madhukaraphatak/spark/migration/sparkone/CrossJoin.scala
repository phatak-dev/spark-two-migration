package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.catalyst.expressions.Literal
object CrossJoin {

  def main(args: Array[String]):Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)

    val loadedDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../test_data/sales.csv")

    //cross join the data
    val crossJoinDf = loadedDf.join(loadedDf)
   // count the joined df
    println(crossJoinDf.count)
  }
}
