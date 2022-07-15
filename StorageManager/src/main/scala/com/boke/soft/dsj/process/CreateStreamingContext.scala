package com.boke.soft.dsj.process

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CreateStreamingContext {

  /**
   * 创建sparkstreamin的运行环境和上下文环境对象
   * @return
   */
  def getSSC(appName:String): StreamingContext ={
    val conf = new SparkConf().setMaster("local[4]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(4))
    ssc.checkpoint("H:\\Project\\scalaworkspace\\StorageManagerSystem\\StorageManager\\src\\main\\checkpoint")
    ssc
  }

}
