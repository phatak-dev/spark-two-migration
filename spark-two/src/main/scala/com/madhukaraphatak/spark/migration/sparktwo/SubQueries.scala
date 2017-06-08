package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SubQueries {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
    val loadedDf = sparkSession.read.format("csv").option("header", "true").option("inferSchema","true").load("../test_data/sales.csv")

    loadedDf.createOrReplaceTempView("sales")
    //add max_amount to each row of the df
    val dfWithMaxAmount = sparkSession.sql("select *, (select max(amountPaid) from sales) max_amount from sales")
    dfWithMaxAmount.show()

    // Add max sales per item
    val dfWithMaxPerItem = sparkSession.sql("select A.itemId, (select max(amountPaid) from sales where itemId=A.itemId) max_amount from sales A") 
    dfWithMaxPerItem.explain(true)
//    dfWithMaxPerItem.show()
 }
}
