package com.wyh.spark.conf

import org.apache.spark.sql.SparkSession

/**
 * @author WangYuhang
 * @since 2022-05-12 14:02
 * */
object SparkConfTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("setMonth", "2")
      .getOrCreate()

    val str = spark.sparkContext.getConf.get("setMonth", null)

    println(str)

    println(spark.sparkContext.getConf.getAll.toList)








  }
}
