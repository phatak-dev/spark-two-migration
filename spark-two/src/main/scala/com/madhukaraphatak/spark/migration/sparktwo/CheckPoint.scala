package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer

object CheckPoint {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
    //set checkpoint dir
    sparkSession.sparkContext.setCheckpointDir("/tmp")
    val loadedDf = sparkSession.read.format("csv").option("header", "true").load("../test_data/sales.csv")
    //iterative algorithm changing df
    val startTime = System.currentTimeMillis()
    (0 to 500).foldRight(loadedDf)((i, df) ⇒ {
      val stringIndexer = new StringIndexer()
      val columnName = "amountPaid"
      stringIndexer.setInputCol(columnName)
      stringIndexer.setOutputCol(s"$columnName$i")
      val newDf = stringIndexer.fit(df).transform(df)
      if (i % 20 == 0) newDf.checkpoint else
        newDf
    })
    val endTime = System.currentTimeMillis()
    println("total time is " + (endTime - startTime) + " in milli seconds")
  }

}
