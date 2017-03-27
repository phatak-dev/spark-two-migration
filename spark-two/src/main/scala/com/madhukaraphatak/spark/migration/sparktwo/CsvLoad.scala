package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CsvLoad {

	def main(args:Array[String]) = {

		val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
		val loadedDF = sparkSession.read.format("csv").option("header","true").load("../test_data/sales.csv")
		loadedDF.show()

	}
}
