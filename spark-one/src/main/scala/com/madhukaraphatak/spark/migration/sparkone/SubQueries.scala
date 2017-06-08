package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SubQueries {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    // Using 2007.csv from http://stat-computing.org/dataexpo/2009/the-data.html
    val loadedDf = sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").option("inferSchema","true").load("../test_data/sales.csv")

    loadedDf.registerTempTable("sales")

    //add max_amount to dataframe

    val max_amount = sqlContext.sql("select max(amountPaid) as max_amount from sales").first.getDouble(0)
    val dfWithMaxAmount = sqlContext.sql(s"select *, ($max_amount) as max_amount from sales")
    dfWithMaxAmount.show()

    // Add max amount per item id using left outer join

    val dfWithMaxPerItem = sqlContext.sql("""select A.itemId, B.max_amount  from sales A left outer join ( select itemId, max(amountPaid) max_amount 
            from sales B group by itemId) B where A.itemId = B.itemId""")
    dfWithMaxPerItem.show()
  }
}
