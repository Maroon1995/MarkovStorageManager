package com.boke.soft.dsj.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

import java.net.URI

class MonitorStopSpark(ssc: StreamingContext) extends Runnable {
  override def run(): Unit = {
    // hdfs文件连接
    val fs: FileSystem = FileSystem.get(new URI("hdfs://master:9000"), new Configuration, "storage")
    //
    while (true) {
      try
        Thread.sleep(5000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      // 获取当前ssc的状态
      val state: StreamingContextState = ssc.getState()
      // 判断hdfs中是否有stopSpark
      val bool = fs.exists(new Path("hdfs://master:9000/monitorspark/stopSpark"))
      if (bool) {
        if (state == StreamingContextState.ACTIVE) {
          // 停止sparkstreaming程序
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          // 退出线程
          System.exit(0)
        }
      }
    }
  }
}
