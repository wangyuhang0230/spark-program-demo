package com.wyh.spark.sql.jdbc

import com.wyh.spark.BaseLocalSpark
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
//      .option("url", "jdbc:mysql://10.10.53.25:2883/test")
////      .option("driver", "org.mariadb.jdbc.Driver")
//      .option("dbtable", "dss_test")
//      .option("user", "root@dora#obcluster")
//      .option("password", "ThtfA25__600100")
//      .load()

    val obUrl = prop.getProperty("obUrl")
    val table = prop.getProperty("ob.table")
    val packet = prop.getProperty("ob.packet")
    val saveMode = prop.getProperty("ob.save.mode")

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
      .mode(saveMode)
      .option("url", obUrl)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", table)
      .option("user", "root@dora#obcluster")
      .option("password", "ThtfA25__600100")
      .option("isolationLevel", "NONE")
      .option("max_allowed_packet", packet)
      .save()




  }
}
