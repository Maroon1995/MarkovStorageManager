package com.boke.soft.dsj.process

import org.apache.spark.{SparkConf, SparkContext}

object CreateSparkContext {

  def getSC(appName:String): SparkContext ={
    // 创建运行环境和上下文环境对象
    val conf = new SparkConf().setMaster("local[4]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("H:\\Project\\scalaworkspace\\StorageManagerSystem\\StorageManager\\src\\main\\checkpoint")
    sc
  }

}
