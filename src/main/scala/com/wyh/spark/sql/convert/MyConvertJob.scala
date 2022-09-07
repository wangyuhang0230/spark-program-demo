package com.wyh.spark.sql.convert

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * 共同卖家数量
 *
 * @author WangYuhang
 * @since 2022-09-07 09:27
 * */
object MyConvertJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._

    val df = spark.sparkContext.makeRDD(Seq(
      ("001", Set("aaa", "bbb", "ccc", "ddd")),
      ("002", Set("aaa", "bbb", "ddd"))
    )).toDF("buyer_id", "seller_id")

    df.show()
    df.createTempView("relation")

    spark.sql(
      """
        |select
        | buyer_id,
        | tmp.seller
        |from relation
        |lateral view explode(seller_id) tmp as seller
        |""".stripMargin).show()

    spark.sql(
      """
        |with t1 as (
        | select
        |  buyer_id,
        |  tmp.seller
        | from relation
        | lateral view explode(seller_id) tmp as seller
        |)
        |select distinct count(seller) from t1 group by seller having count(buyer_id) > 1
        |""".stripMargin).show()

    spark.sql(
      """
        |with t1 as (
        | select
        |  buyer_id,
        |  tmp.seller
        | from relation
        | lateral view explode(seller_id) tmp as seller
        |),
        |t2 as (
        | select seller, count(buyer_id) from t1 group by seller having count(buyer_id) > 1
        |)
        |select count(*) from t2
        |""".stripMargin).show()



  }
}
