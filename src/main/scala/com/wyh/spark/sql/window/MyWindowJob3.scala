package com.wyh.spark.sql.window

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

/**
 * 统计直播间人员变动情况
 *
 * 源表：
  +-------+-------+-------------------+-------------------+
  |room_id|user_id|         start_time|           end_time|
  +-------+-------+-------------------+-------------------+
  |      a|  user1|2022-02-01 10:12:13|2022-02-01 10:30:23|
  |      a|  user2|2022-02-01 10:21:10|2022-02-01 11:02:06|
  +-------+-------+-------------------+-------------------+
 *目标表：
  +-------+-------------------+-------------------+--------+
  |room_id|         start_time|           end_time|user_cnt|
  +-------+-------------------+-------------------+--------+
  |      a|2022-02-01 10:12:13|2022-02-01 10:21:10|       1|
  |      a|2022-02-01 10:21:10|2022-02-01 10:30:23|       2|
  |      a|2022-02-01 10:30:23|2022-02-01 11:02:06|       1|
  +-------+-------------------+-------------------+--------+
 *
 * @author WangYuhang
 * @since 2022-07-09 12:07
 */
object MyWindowJob3 {

  case class UserAction(room_id: String, user_id: String, start_time: Timestamp, end_time: Timestamp)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._

    val userDF = spark.sparkContext.makeRDD(Seq(
      ("a", "user1", "2022-02-01 10:12:13", "2022-02-01 10:30:23"),
      ("a", "user2", "2022-02-01 10:21:10", "2022-02-01 11:02:06")
    )).toDF("room_id", "user_id", "start_time", "end_time")
      .as[UserAction]

    userDF.show()
    userDF.createTempView("user")

    spark.sql(
      s"""
         |select room_id, start_time change_time, 1 cnt from user
         |union
         |select room_id, end_time change_time, -1 cnt from user
         |order by change_time
         |""".stripMargin).show()

//    spark.sql(
//      s"""
//         |select
//         |room_id, change_time, cnt
//         |from (
//         |
//         |select room_id, start_time change_time, 1 cnt from user
//         |union
//         |select room_id, end_time change_time, -1 cnt from user
//         |
//         |)
//         |""".stripMargin).show()

    spark.sql(
      s"""
         |select * from
         |(
         |select
         |room_id,
         |first(change_time) over(order by change_time rows between current row and 1 following) as start_time,
         |last(change_time) over(order by change_time rows between current row and 1 following) as end_time,
         |sum(cnt) over(order by change_time rows between unbounded preceding and current row) as user_cnt
         |from
         |(
         |select room_id, start_time change_time, 1 cnt from user
         |union
         |select room_id, end_time change_time, -1 cnt from user
         |)
         |)
         |where start_time != end_time
         |
         |""".stripMargin).show()






  }
}
