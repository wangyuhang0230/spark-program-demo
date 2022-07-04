package com.wyh.spark.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * 开窗函数
 * 统计连续活跃天数
 *
 * @author WangYuhang
 * @since 2022-06-17 14:59
 * */
object MyWindowJob2 {
  case class Record(uid: String, dates: Timestamp){
    def this(uid: String, dates: LocalDateTime){
      this(uid, Timestamp.valueOf(dates))
    }
  }
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("error")
//    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val df = spark.sparkContext.makeRDD(Seq(
      ("uid001", "2021-07-21"),
      ("uid001", "2021-07-22"),
      ("uid001", "2021-07-23"),
      ("uid001", "2021-07-24"),
      ("uid001", "2021-07-25"),
      ("uid001", "2021-07-26"),
      ("uid001", "2021-07-27"),
      ("uid001", "2021-07-28"),
      ("uid002", "2021-07-22"),
      ("uid002", "2021-07-23"),
      ("uid002", "2021-08-04"),
      ("uid002", "2021-08-05"),
      ("uid002", "2021-08-06"),
      ("uid002", "2021-08-07"),
      ("uid002", "2021-08-08"),
      ("uid003", "2021-08-10"),
      ("uid003", "2021-08-14"),
      ("uid003", "2021-08-15"),
      ("uid003", "2021-08-16"),
      ("uid003", "2021-08-17"),
      ("uid003", "2021-08-18")
    )).toDF("uid", "dates").as[Record]

    df.createTempView("records")
    df.show()

    spark.sql(
      s"""
         |SELECT  uid
         |          ,dates
         |          ,ROW_NUMBER() OVER(PARTITION BY uid ORDER BY dates ASC) AS rn
         |          ,first_value(dates)over(PARTITION BY uid ORDER BY dates ASC) fir
         |          ,first_value(dates)over(PARTITION BY uid ORDER BY dates desc) las1
         |          ,last_value(dates)over(PARTITION BY uid ORDER BY dates ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) las
         |          ,last_value(dates)over(PARTITION BY uid ORDER BY dates ASC) las2
         |FROM records
         |""".stripMargin).show(100)
//
//    spark.sql(
//      s"""
//         |SELECT  uid
//         |       ,dates
//         |       ,rn
//         |       ,fir
//         |       ,las1
//         |       ,las
//         |       ,date_sub(dates ,rn-1) keycolumn
//         |       from (
//         |SELECT  uid
//         |          ,dates
//         |          ,ROW_NUMBER() OVER(PARTITION BY uid ORDER BY dates ASC) AS rn
//         |          ,first_value(dates)over(PARTITION BY uid ORDER BY dates ASC) fir
//         |          ,first_value(dates)over(PARTITION BY uid ORDER BY dates desc) las1
//         |          ,last_value(dates)over(PARTITION BY uid ORDER BY dates ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) las
//         |FROM records
//         |)
//         |""".stripMargin).show(100)
//
//
//    spark.sql(s"""
//                 |SELECT  uid
//                 |        ,fir
//                 |        ,las
//                 |        ,keycolumn
//                 |        ,count(dates)
//                 |        ,min(dates)
//                 |        ,max(dates)
//                 |from (
//                 |
//                 |SELECT  uid
//                 |       ,dates
//                 |       ,rn
//                 |       ,fir
//                 |       ,las1
//                 |       ,las
//                 |       ,date_sub(dates ,rn-1) keycolumn
//                 |       from (
//                 |SELECT  uid
//                 |          ,dates
//                 |          ,ROW_NUMBER() OVER(PARTITION BY uid ORDER BY dates ASC) AS rn
//                 |          ,first_value(dates)over(PARTITION BY uid ORDER BY dates ASC) fir
//                 |          ,first_value(dates)over(PARTITION BY uid ORDER BY dates desc) las1
//                 |          ,last_value(dates)over(PARTITION BY uid ORDER BY dates ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) las
//                 |FROM records
//                 |)
//                 |)
//                 |GROUP BY uid
//                 |         ,fir
//                 |         ,las
//                 |         ,keycolumn
//                 |""".stripMargin).show(100)
//

    while (true) {}
  }
}
