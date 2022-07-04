package com.wyh.spark

import org.apache.spark.sql.SparkSession

object SqlJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .getOrCreate()
  }




}
