package com.wyh.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingJob {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Streaming-wyh").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))



  }
}
