package com.wyh.spark.sql.jdbc

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author WangYuhang
 * @since 2022-10-27 10:35
 * */
object Spark2OBJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._

    val dssTestDf = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.10.53.25:2883/test")
//      .option("driver", "org.mariadb.jdbc.Driver")
      .option("dbtable", "dss_test")
      .option("user", "root@dora#obcluster")
      .option("password", "ThtfA25__600100")
      .load()

    dssTestDf.show()


//    val df = spark.sparkContext.makeRDD(Seq(
//      (5, "eee"),
//      (6, "fff"),
//      (7, "ggg"),
//      (8, "hhh")
//    )).toDF("id", "name")

    val df = spark.sparkContext.makeRDD(Seq(
      "iii", "jjj", "kkk"
    )).toDF("name")

    df.show()

    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:mysql://10.10.53.25:2883/test")
      .option("dbtable", "dss_test")
      .option("user", "root@dora#obcluster")
      .option("password", "ThtfA25__600100")
      .option("isolationLevel", "NONE")
      .save()




  }
}
