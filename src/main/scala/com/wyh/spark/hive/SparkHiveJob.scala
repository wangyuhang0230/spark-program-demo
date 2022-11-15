package com.wyh.spark.hive

import org.apache.spark.sql.SparkSession

import java.util.Scanner

/**
 * @author WangYuhang
 * @since 2022-11-14 15:31
 * */
object SparkHiveJob {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    import spark.implicits._
    import spark.sql

    sql("use hn_test")

    sql("select id from test").show()


    val scanner = new Scanner(System.in)
    val str = scanner.nextLine()
  }
}
