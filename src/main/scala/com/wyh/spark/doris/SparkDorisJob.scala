package com.wyh.spark.doris

import com.wyh.spark.BaseLocalSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

/**
 * @author WangYuhang
 * @since 2023-03-03 10:14
 * */
object SparkDorisJob extends BaseLocalSpark {
  def main(args: Array[String]): Unit = {

    val url = prop.getProperty("doris.url")
    val username = prop.getProperty("doris.username")
    val password = prop.getProperty("doris.password")
    val table = prop.getProperty("doris.table")
    val fenodes = prop.getProperty("doris.fenodes")
    val dbTable = prop.getProperty("doris.db.table")
    val saveMode = prop.getProperty("doris.save.mode")

    import spark.implicits._

//    spark.sparkContext.setLogLevel("debug")

    val hisData: DataFrame = spark.read.format("csv")
      .option("header", "true")
//      .option("sep", "\t")
//      .option("nullValue", "\\N")
      .schema(StructType(
        StructField("UPDATE_TIME", LongType, nullable = false) ::
          StructField("POINT_ID", StringType, nullable = false) ::
          StructField("UPLOAD_TIME", LongType, nullable = true) ::
          StructField("VAL", DoubleType, nullable = true) ::
          StructField("QUALITY_TYPE", IntegerType, nullable = true) ::
          StructField("VALID_RANGE", StringType, nullable = true) ::
          StructField("ZERO_DRIFT", DoubleType, nullable = true) :: Nil)
      )
      .load("/user/wyh/hn/data/his_02004_2022")
//      .load("/user/wyh/test/his.csv")
      .withColumn("UPDATE_TIME", ($"UPDATE_TIME" / 1000).cast(TimestampType))
      .withColumn("UPLOAD_TIME", ($"UPLOAD_TIME" / 1000).cast(TimestampType))
//      .withColumn("UPDATE_TIME", from_unixtime($"UPDATE_TIME" / 1000))
//      .withColumn("UPLOAD_TIME", from_unixtime($"UPLOAD_TIME" / 1000))

//    hisData.schema.printTreeString()
//    hisData
    hisData.show()

    // jdbc 方式
//    spark.read.format("jdbc")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", url)
//      .option("dbtable", "t_user")
//      .option("user", username)
//      .option("password", password)
//      .load()
//      .show()

    // Doris 连接器方式
//    val hisData = spark.read.format("doris")
//      .option("doris.table.identifier", dbTable)
//      .option("doris.fenodes", fenodes)
//      .option("user", username)
//      .option("password", password)
//      .load()

//    val create = """CREATE TABLE IF NOT EXISTS test.his_02004_2022
//                   |(
//                   |    `update_time` DATETIME NOT NULL,
//                   |    `point_id` VARCHAR(100) NOT NULL,
//                   |    `upload_time` DATETIME,
//                   |    `val` DOUBLE,
//                   |    `quality_type` INT,
//                   |    `valid_range` VARCHAR,
//                   |    `zero_drift` DOUBLE
//                   |)
//                   |UNIQUE KEY(`update_time`, `point_id`)
//                   |DISTRIBUTED BY HASH(`update_time`, `point_id`) BUCKETS 12;""".stripMargin

    // jdbc 方式
//    hisData.write.format("jdbc")
//      .mode(saveMode)
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("url", url)
//      .option("dbtable", table)
//      .option("user", username)
//      .option("password", password)
//      .save()

    // Doris 连接器方式
    hisData
      .write.format("doris")
//      .mode(saveMode)
      .option("doris.table.identifier", dbTable)
      .option("doris.fenodes", fenodes)
      .option("sink.batch.size", "50000")
      .option("sink.max-retries", "1")
//      .option("sink.properties.column_separator", ",")
//      .option("sink.properties.line_delimiter", ",")
      .option("user", username)
      .option("password", password)
      .save()



  }
}
