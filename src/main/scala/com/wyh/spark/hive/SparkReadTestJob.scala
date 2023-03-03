package com.wyh.spark.hive

import com.wyh.spark.BaseLocalSpark

/**
 * @author WangYuhang
 * @since 2023-03-01 09:35
 * */
object SparkReadTestJob extends BaseLocalSpark{
  def main(args: Array[String]): Unit = {

    val zkUrl = prop.getProperty("phoenix.zkUrl")
    val hive_table = prop.getProperty("hive.table")
    val phoenix_table = prop.getProperty("phoenix.table")
    val month = prop.getProperty("hive.select.month").toInt

    import spark.sql

    sql(s"select count(*) hive_count from ${hive_table} where m = $month").show()

    val phoenixDF = spark.read
//      .format("org.apache.phoenix.spark")
//      .option("zkUrl", zkUrl)
//      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
//      .option("table", table)
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", zkUrl)
      .option("dbtable", phoenix_table)
      .load()
      .filter(s"update_time >= to_timestamp('2022-0$month-01 00:00:00')")
      .filter(s"update_time < to_timestamp('2022-0${month + 1}-01 00:00:00')")
      .selectExpr("count(*) as phoenix_count")

    phoenixDF.explain()
//
//    phoenixDF.show()




  }
}
