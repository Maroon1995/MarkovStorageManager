package com.boke.soft.dsj.process

import com.alibaba.druid.pool.DruidPooledConnection
import com.boke.soft.dsj.common.MyMath
import com.boke.soft.dsj.io.{HBaseReader, MysqlReader}
import com.boke.soft.dsj.produce.Produce
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object CreateSparkContext {

  def getSC(appName:String): SparkContext ={
    // 创建运行环境和上下文环境对象
    val conf = new SparkConf().setMaster("local[4]").setAppName(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 注册要序列化的类
    // 注意：即使使用 Kryo 序列化，也要继承 Serializable 接口
    conf.registerKryoClasses(Array(classOf[Produce],classOf[HBaseReader],classOf[MysqlReader],classOf[MyMath],classOf[DruidPooledConnection]))
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("H:\\Project\\scalaworkspace\\StorageManagerSystem\\StorageManager\\src\\main\\checkpoint")
    sc
  }

}
