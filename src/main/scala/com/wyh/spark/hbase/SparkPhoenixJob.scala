package com.wyh.spark.hbase

import com.wyh.spark.BaseLocalSpark
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author WangYuhang
 * @since 2023-02-28 16:08
 * */
object SparkPhoenixJob extends BaseLocalSpark{
  def main(args: Array[String]): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val zkUrl = prop.getProperty("phoenix.zkUrl")
    val table = prop.getProperty("phoenix.table")

    val hisData: DataFrame = spark.read.format("csv")
      .option("header", "true")
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
      .withColumn("UPDATE_TIME", ($"UPDATE_TIME" / 1000).cast(TimestampType))
      .withColumn("UPLOAD_TIME", ($"UPLOAD_TIME" / 1000).cast(TimestampType))

    hisData.schema.printTreeString()

    if (runAtWindows) {
      hisData.show()
    } else {
      hisData.write
        .format("org.apache.phoenix.spark")
        // 只支持 Overwrite 模式
        .mode(SaveMode.Overwrite)
        .options(Map("table" -> table, "zkUrl" -> zkUrl))
        .save()
    }

  }
}
