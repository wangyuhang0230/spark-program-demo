package com.wyh.spark.sql.jdbc

import com.wyh.spark.BaseLocalSpark
import com.wyh.spark.conf.BaseConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author WangYuhang
 * @since 2022-10-27 10:35
 * */
object Spark2OBJob extends BaseLocalSpark{
  def main(args: Array[String]): Unit = {

    import spark.implicits._

//    val dssTestDf = spark.read
//      .format("jdbc")
//      .option("url", BaseConf.ob_url)
////      .option("driver", "org.mariadb.jdbc.Driver")
//      .option("dbtable", BaseConf.ob_table)
//      .option("user", BaseConf.ob_username)
//      .option("password", BaseConf.ob_password)
//      .load()

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

    hisData.write
      .format("jdbc")
      .mode(BaseConf.ob_saveMode)
      .option("url", BaseConf.ob_url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", BaseConf.ob_table)
      .option("user", BaseConf.ob_username)
      .option("password", BaseConf.ob_password)
      .option("isolationLevel", "NONE")
      .option("max_allowed_packet", BaseConf.ob_packet)
      .save()




  }
}
