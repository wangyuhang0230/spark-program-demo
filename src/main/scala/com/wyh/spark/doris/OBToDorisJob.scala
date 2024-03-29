package com.wyh.spark.doris

import com.wyh.spark.BaseLocalSpark
import com.wyh.spark.conf.BaseConf

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.Properties

import org.apache.spark.sql.functions._

/**
 * @author WangYuhang
 * @since 2023-05-26 14:22
 * */
object OBToDorisJob extends BaseLocalSpark{
  def main(args: Array[String]): Unit = {

//    val obDF = spark.read
//      .format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", BaseConf.ob_url)
//      .option("dbtable", BaseConf.ob_table)
//      .option("user", BaseConf.ob_username)
//      .option("password", BaseConf.ob_password)
//      .option("partitionColumn", "update_time")
//      .option("lowerBound", 10000)
//      .option("upperBound", 1000000)
//      .option("numPartitions", 12)
//      .option("isolationLevel", "NONE")
//      .load()

    val interval = 1
    val array = (1 to (366*4/interval) + 1)
      .map(i => {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val time = LocalDate.ofYearDay(2022, 1).atTime(0, 0, 0)
//        val startDate = time.plusDays((i - 1) * interval)
//        val endDate = startDate.plusDays(interval)
        val startDate = time.plusHours((i - 1) * interval * 6)
        val endDate = startDate.plusHours(interval * 6)
        s"update_time >= '${formatter.format(startDate)}' and update_time < '${formatter.format(endDate)}'"
      })
      .toArray

    println(BaseConf.ob_table + " ===> " + BaseConf.doris_dbTable)

    println("array:")
    println(array.mkString("\n"))

    val properties = new Properties();
    properties.put("user", BaseConf.ob_username)
    properties.put("password", BaseConf.ob_password)
    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    properties.put("max_allowed_packet", BaseConf.ob_packet)
    properties.put("fetchsize", "50000")
    properties.put("readBatchSize", "50000")
    properties.put("useCursorFetch", "true")

    val obDF = spark.read
      .option("fetchsize", "50000")
      .jdbc(BaseConf.ob_url, BaseConf.ob_table, array, properties)
      .withColumn("quality_type", lit(null))
      .withColumn("valid_range", lit(null))
      .withColumn("zero_drift", lit(null))

    println("------------------------")
//    obDF.show()

    // Doris 连接器方式
    obDF
      .write.format("doris")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("doris.table.identifier", BaseConf.doris_dbTable)
      .option("doris.fenodes", BaseConf.doris_fenodes)
      .option("sink.batch.size", "500000")
      .option("sink.max-retries", "1")
      .option("user", BaseConf.doris_username)
      .option("password", BaseConf.doris_password)
      .save()

  }
}
