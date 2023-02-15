package com.wyh.spark

import org.apache.spark.sql.SparkSession

/**
 * @author WangYuhang
 * @since 2023-02-15 11:04
 * */
class BaseLocalSpark extends BaseFunctions {
  System.setProperty("HADOOP_USER_NAME", "hdfs")

  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("error")
}