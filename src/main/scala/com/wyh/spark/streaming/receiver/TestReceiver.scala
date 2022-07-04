package com.wyh.spark.streaming.receiver

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.control.NonFatal

/**
 * @author WangYuhang
 * @since 2022-06-17 14:59
 */
class TestReceiver extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY) with Logging{
  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {

  }

  def receive(): Unit = {
    try {
      var i = 0
      while(!isStopped) {
        Thread.sleep(5000)
        store(Thread.currentThread().getId + " - i = " + i)
        i = i + 1
      }
      if (!isStopped()) {
        restart("Socket data stream had no more data")
      } else {
        logInfo("Stopped receiving")
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Error receiving data", e)
        restart("Error receiving data", e)
    } finally {
      onStop()
    }
  }
}
