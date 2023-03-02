package com.wyh.spark.hive

import com.wyh.spark.BaseLocalSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * @author WangYuhang
 * @since 2023-02-15 11:18
 * */
object SparkHDFSJob extends BaseLocalSpark {
  def main(args: Array[String]): Unit = {

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

    hisData.schema.printTreeString()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val resultDF: DataFrame = hisData
      .withColumn("UPDATE_TIME", from_unixtime($"UPDATE_TIME" / 1000))
      .withColumn("UPLOAD_TIME", ($"UPLOAD_TIME" / 1000).cast(TimestampType))

    resultDF.schema.printTreeString()
    resultDF.show()

    val count = spark.sparkContext.longAccumulator("count")
    resultDF.foreach(row => count.add(1))
    println("count: " + count.value)



    //  awaitCommand()

  }
}
