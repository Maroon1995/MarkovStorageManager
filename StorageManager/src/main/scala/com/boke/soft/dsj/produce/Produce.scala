package com.boke.soft.dsj.produce

import com.boke.soft.dsj.bean.MaterialQuantityInfo
import com.boke.soft.dsj.io.HBaseReader
import com.boke.soft.dsj.util.DateUtil.getDateMonths
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Produce {

  var historyTime: String = getDateMonths(66, "yyyy/MM")
  var currentTime: String = getDateMonths(0, "yyyy/MM")

  /**
   * 从HBase中读取表ORIGINAL_DATA数据
   */
  def MaterialQuantityRDD(sc:SparkContext): RDD[MaterialQuantityInfo] ={
    val sql =
      s"""
         |select "item_cd", "item_desc","insert_datetime","quantity"
         |from "ORIGINAL_DATA"
         |where "insert_datetime" >= '${historyTime}'
         |and "insert_datetime" <= '${currentTime}'
         |""".stripMargin
    val hbaseReader = new HBaseReader(sc)
    val MaterialQuantityInfoRDD: RDD[MaterialQuantityInfo] = hbaseReader.toMaterialQuantityInfoRDD(sql)
    MaterialQuantityInfoRDD
  }

  /**
   * 从HBase中读取表QUANTITY_STATUS数据
   */
  def MaterialQuantityStatusRDD(sc:SparkContext): RDD[MaterialQuantityInfo]={
    val sql =
      s"""
         |select "item_cd", "item_desc","insert_datetime","quantity","status"
         |from "QUANTITY_STATUS"
         |where "insert_datetime" >= '${historyTime}'
         |and "insert_datetime" <= '${currentTime}'
         |""".stripMargin
    val hbaseReader = new HBaseReader(sc)
    val MaterialQuantityStatusRDD: RDD[MaterialQuantityInfo] = hbaseReader.toMaterialQuantityStatusRDD(sql)
    MaterialQuantityStatusRDD
  }

}
