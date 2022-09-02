package com.wyh.spark.sql.convert

import org.apache.spark.sql.SparkSession

/**
 * @author WangYuhang
 * @since 2022-08-23 16:40
 * */
object ConvertColAndRow {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._

    val grade = spark.sparkContext.makeRDD(Seq(
      ("张三", "数学", 34),
      ("张三", "语文", 58),
      ("张三", "英语", 58),
      ("李四", "数学", 45),
      ("李四", "语文", 87),
      ("李四", "英语", 45),
      ("王五", "数学", 76),
      ("王五", "语文", 34),
      ("王五", "英语", 89)
    )).toDF("user_name", "course", "score")

    grade.createTempView("test_tb_grade")

    // 1.1 pivot
    val grade1 = grade.groupBy("user_name").pivot("course").max("score")

    // spark 2.4
//    val grade1 = spark.sql(
//      """
//        |select * from test_tb_grade
//        |pivot
//        |(
//        |  max(score) for
//        |  course in ('数学', '语文', '英语')
//        |)
//        |""".stripMargin)

    grade1.show()

    grade1.createTempView("test_tb_grade1")

    // 1.2 stack

    spark.sql(
      """
        |select
        | user_name,
        | stack(3, '数学', `数学`, '语文', `语文`, '英语', `英语`) as (course, score)
        |from test_tb_grade1
        |""".stripMargin).show()


    // 2.1 case when | if
    val grade2 = spark.sql(
      """
        |select
        | user_name,
        | max(case course when '数学' then score else 0 end) `数学`,
        | max(case course when '语文' then score else 0 end) `语文`,
        | max(case course when '英语' then score else 0 end) `英语`
        |from test_tb_grade group by user_name
        |""".stripMargin)

    grade2.show()

    grade2.createTempView("test_tb_grade2")

    // 2.2 union

    spark.sql(
      """
        |select user_name, '数学' course, `数学` score from test_tb_grade2
        |union
        |select user_name, '语文' course, `语文` score from test_tb_grade2
        |union
        |select user_name, '英语' course, `英语` score from test_tb_grade2
        |""".stripMargin).show()


    // 3.1 聚合成列
    val grade3 = spark.sql(
      """
        |select
        | user_name,
        | collect_set(course) set_course,
        | sum(score) sum_score
        |from test_tb_grade group by user_name
        |""".stripMargin)

    grade3.show()

    // 3.2 炸裂成行
    grade3.createTempView("test_tb_grade3")

    spark.sql(
      """
        |select
        | user_name,
        | sum_score,
        | tmp.course
        |from test_tb_grade3
        |lateral view explode(set_course) tmp as course
        |-- lateral view explode(split(concat_course, ',')) tmp as course
        |""".stripMargin).show()







  }
}
