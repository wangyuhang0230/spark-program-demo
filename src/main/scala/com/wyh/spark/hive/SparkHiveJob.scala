package com.wyh.spark.hive

import com.wyh.spark.BaseLocalSpark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

/**
 * @author WangYuhang
 * @since 2022-11-14 15:31
 * */
object SparkHiveJob extends BaseLocalSpark {
  def main(args: Array[String]): Unit = {

    import spark.sql
    import spark.implicits._
    import org.apache.spark.sql.functions._

    sql("use hn_test")

    // 创建外部表，管理csv文件
    sql(
      """create external table if not exists his_02004_2022_txt(
        |UPDATE_TIME string,
        |POINT_ID string,
        |UPLOAD_TIME string,
        |VAL double,
        |QUALITY_TYPE integer,
        |VALID_RANGE string,
        |ZERO_DRIFT double
        |)
        |row format delimited fields terminated by ','
        |stored as textfile
        |location '/user/wyh/test'
        |tblproperties("skip.header.line.count"="1")""".stripMargin)

    // 创建分区表，存储历史数据
    sql("""CREATE external TABLE if not exists `his_02004_2022_p_orc`(
          |	`update_time` timestamp,
          |	`point_id` string,
          |	`upload_time` timestamp,
          |	`val` double,
          |	`quality_type` int,
          |	`valid_range` string,
          |	`zero_drift` double)
          |PARTITIONED BY (`m` int)
          |LOCATION 'hdfs://nn.hadoop:8020/warehouse/tablespace/external/hive/hn_test.db/his_02004_2022_p_orc'
          |TBLPROPERTIES ('transactional'='false')""".stripMargin)

    val cleaningDF = sql(
      s"""select
         |*,
         |${month(($"update_time".cast(LongType) / 1000).cast(TimestampType))} m
         |from his_02004_2022_txt where update_time != 'UPDATE_TIME'""".stripMargin)
      .withColumn("update_time", ($"update_time".cast(LongType) / 1000).cast(TimestampType))
      .withColumn("upload_time", ($"upload_time".cast(LongType) / 1000).cast(TimestampType))
//      .limit(10)

//    cleaningDF.show()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    cleaningDF
      .write
      .format("Hive")
      .mode(SaveMode.Append)
      .partitionBy("m")
      .saveAsTable("his_02004_2022_p_orc")




//    awaitCommand()

  }
}
