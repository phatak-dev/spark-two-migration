package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer

object CheckPoint {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val loadedDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("../test_data/sales.csv")
    val startTime = System.currentTimeMillis()
    //iterative algorithm changing df

    (0 to 500).foldRight(loadedDf)((i, df) ⇒ {
      val stringIndexer = new StringIndexer()
      val columnName = "amountPaid"
      stringIndexer.setInputCol(columnName)
      stringIndexer.setOutputCol(s"$columnName$i")
      val newDf = stringIndexer.fit(df).transform(df)
      newDf
    })
    val endTime = System.currentTimeMillis()
    println("total time is " + (endTime - startTime) + " in milli seconds")
  }
}
