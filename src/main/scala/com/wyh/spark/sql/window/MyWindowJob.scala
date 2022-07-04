package com.wyh.spark.sql.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * 开窗函数
 * 统计当天增长值
 *
 * @author WangYuhang
 * @since 2022-06-17 14:59
 * */
object MyWindowJob {
  case class Record(date: Timestamp, v: Int){
    def this(date: LocalDateTime, v: Int){
      this(Timestamp.valueOf(date), v)
    }
  }
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("error")
//    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val time = LocalDateTime.of(2022, 6, 1, 0, 0, 0)
    val v = 100

    val df = spark.sparkContext.makeRDD(Seq(
      new Record(time, v),
      new Record(time.plusDays(1), v + 20),
      new Record(time.plusDays(2), v + 70),
      new Record(time.plusDays(3), v + 90),
      new Record(time.plusDays(4), v + 200)
    )).toDF()

    df.createTempView("records")
    df.show()

    spark.sql(
      """
        |select date, v, sum(v) over() as r
        |from records
        |""".stripMargin)
      .show(false)

    df.withColumn(
        "r",
        last("v")
          .over(
            Window.orderBy($"date").rowsBetween(Window.currentRow, 1)
          )
          .minus($"v")
      )
      .show(false)



    while (true) {}
  }
}
