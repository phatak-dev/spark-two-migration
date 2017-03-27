
package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DFMapExample {

	def main(args:Array[String]) = {

		val sparkSession = SparkSession.builder.master("local").appName("mapexample").getOrCreate()
		val loadedDF = sparkSession.read.format("csv").option("header","true").option("inferSchema","true").load("../test_data/sales.csv")
		//rdd way
		val amountRDD = loadedDF.rdd.map(row => row.getDouble(3))
		println(amountRDD.collect.toList)

		//dataset way
		import sparkSession.implicits._
		val amountDataSet = loadedDF.map(row => row.getDouble(3))
		amountDataSet.show()

	}
}
