package com.wyh.spark.hive

import com.wyh.spark.BaseLocalSpark
import com.wyh.spark.conf.BaseConf

/**
 * @author WangYuhang
 * @since 2023-03-01 09:35
 * */
object SparkReadTestJob extends BaseLocalSpark{
  def main(args: Array[String]): Unit = {

    import spark.sql

    sql(s"select count(*) hive_count from ${BaseConf.hive_table} where m = ${BaseConf.select_month}").show()

    val phoenixDF = spark.read
      .format("org.apache.phoenix.spark")
      .option("zkUrl", BaseConf.phoenix_zkUrl)
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("table", BaseConf.phoenix_table)
//      .format("jdbc")
//      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
//      .option("url", BaseConf.phoenix_zkUrl)
//      .option("dbtable", BaseConf.phoenix_table)
      .load()
      .filter(s"update_time >= to_timestamp('2022-0${BaseConf.select_month}-01 00:00:00')")
      .filter(s"update_time < to_timestamp('2022-0${BaseConf.select_month + 1}-01 00:00:00')")
      .selectExpr("count(*) as phoenix_count")

    phoenixDF.explain()
//
//    phoenixDF.show()




  }
}
