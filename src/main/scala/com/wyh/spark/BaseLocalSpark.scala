package com.wyh.spark

import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @author WangYuhang
 * @since 2023-02-15 11:04
 * */
class BaseLocalSpark extends BaseFunctions {

  System.setProperty("HADOOP_USER_NAME", "hive")

  val prop: Properties = {
    val prop = new Properties()
    prop.load(this.getClass.getResourceAsStream("/demo.properties"))
    prop
  }

  val spark: SparkSession = if (runAtWindows) {
    getSpark("local[*]")
  } else {
    getSpark("yarn")
  }

  spark.sparkContext.setLogLevel("error")

  def getRuntimeOS: String ={
    System.getProperty("os.name")
  }

  def runAtWindows: Boolean = getRuntimeOS.contains("Windows")

  def getSpark(master: String): SparkSession = {
    SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master(master)
      .config("hive.metastore.dml.events","false") // 解决 Hive 载入数据 BUG
      .enableHiveSupport()
      .getOrCreate()
  }
}
