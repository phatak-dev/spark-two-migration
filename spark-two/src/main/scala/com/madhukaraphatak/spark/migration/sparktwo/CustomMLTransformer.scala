package com.madhukaraphatak.spark.migration.sparktwo

import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{ Dataset, DataFrame }
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.util.Identifiable

class IdentityTransformer(override val uid: String = Identifiable.randomUID("identity")) extends Transformer {
  override def transform(inputData: Dataset[_]): DataFrame = {
    inputData.toDF()
  }

  override def copy(paramMap: ParamMap) = this
  override def transformSchema(schema: StructType) = schema
}
object CustomMLTransformer {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local").appName("custom transform example").getOrCreate()
    val loadedDf = sparkSession.read.format("csv").option("header", "true").load("../test_data/sales.csv")

    val transformedDf = new IdentityTransformer().transform(loadedDf)
    transformedDf.show()

    //create a ds with single column
    import sparkSession.implicits._
    val ds = loadedDf.select("itemId").as[String]
    val transformedDs = new IdentityTransformer().transform(ds)
    transformedDs.show()
  }
}
