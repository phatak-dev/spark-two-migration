package com.madhukaraphatak.spark.migration.sparkone

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.util.Identifiable

class IdentityTransformer(override val uid:String=Identifiable.randomUID("identity")) extends Transformer {
 override def transform(inputData:DataFrame):DataFrame = {
  inputData 
 }

 override def copy(paramMap:ParamMap) = this
 override def transformSchema(schema:StructType)= schema
}
object CustomMLTransformer {

  def main(args: Array[String]):Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("mapexample")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val loadedDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("../test_data/sales.csv")
    val transformedDf = new IdentityTransformer().transform(loadedDF)
    transformedDf.show()


  }
}
