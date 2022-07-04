package com.wyh.spark.streaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HnStreamingReceiverJob {
  def main(args: Array[String]): Unit = {

    val appName = "Hn-Streaming-Receiver-wyh"
    val master = "local[4]"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    val ssc = new StreamingContext(conf, Seconds(30))
    ssc.sparkContext.setLogLevel("ERROR")

    val receiverStream = ssc.receiverStream(new TestReceiver)
    val receiverStream2 = ssc.receiverStream(new TestReceiver)

//    receiverStream.print()
    receiverStream.union(receiverStream2)
      .repartition(2)
      .map("executor: " + Thread.currentThread().getId + " - " + _)
      .foreachRDD(rdd => {
        rdd.foreach(println)
      })

    println("driver 线程 id: " + Thread.currentThread().getId)

    ssc.start()
    ssc.awaitTermination()
  }
}
