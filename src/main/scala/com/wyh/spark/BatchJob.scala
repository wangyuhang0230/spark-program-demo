package com.wyh.spark

import org.apache.spark.{SparkConf, SparkContext}

object BatchJob {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Batch-wyh").setMaster("local[*]")
    val sc = new SparkContext(conf)

  }
}
