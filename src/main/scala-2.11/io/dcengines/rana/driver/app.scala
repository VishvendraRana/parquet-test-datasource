package io.dcengines.rana.driver

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by rana on 26/9/16.
  */
object app extends App {
  val conf = new SparkConf().setAppName("spark-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("io.dcengines.rana.sql").load("data/")
  df.printSchema()

  df.show()
}
