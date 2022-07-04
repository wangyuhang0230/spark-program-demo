package com.wyh.spark

import org.apache.spark.sql.SparkSession

object StructuredStreamingJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
  }






}
