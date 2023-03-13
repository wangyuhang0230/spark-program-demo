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

    val hiveCount = spark.sparkContext.longAccumulator("hive")
    val dorisCount = spark.sparkContext.longAccumulator("doris")
    val phoenixCount = spark.sparkContext.longAccumulator("phoenix")

    val hiveDF = sql(s"select * from ${BaseConf.hive_table} where m = ${BaseConf.select_month}")
    hiveDF.explain()
    hiveDF.foreach(_ => hiveCount.add(1))
    println("hiveCount: " + hiveCount.value)

    // Doris 连接器方式
    val dorisDF = spark.read.format("doris")
      .option("doris.table.identifier", BaseConf.doris_dbTable)
      .option("doris.fenodes", BaseConf.doris_fenodes)
      .option("user", BaseConf.doris_username)
      .option("password", BaseConf.doris_password)
      .option("doris.request.tablet.size", BaseConf.doris_tablet_size)
      .option("doris.batch.size", BaseConf.doris_batch_size)
      .load()
      .filter(s"update_time >= '2022-0${BaseConf.select_month}-01 00:00:00'")
      .filter(s"update_time < '2022-0${BaseConf.select_month + 1}-01 00:00:00'")

    dorisDF.explain()
    dorisDF.foreach(_ => dorisCount.add(1))
    println("dorisCount: " + dorisCount.value)

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
//      .selectExpr("count(*) as phoenix_count")

    phoenixDF.explain()
    phoenixDF.foreach(_ => phoenixCount.add(1))
    println("phoenixCount: " + phoenixCount.value)



  }
}
