package com.boke.soft.dsj.produce

import com.alibaba.fastjson.JSON
import com.boke.soft.dsj.bean.{DoubleStatusCount, MaterialQuantityInfo, SingleStatusCount}
import com.boke.soft.dsj.common.Transform
import com.boke.soft.dsj.io.{HBaseReader, MysqlReader}
import com.boke.soft.dsj.util.DateUtil.getDateMonths
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

class Produce(spark:SparkSession) extends Serializable {

  var historyTime: String = getDateMonths(66, "yyyy/MM")
  var currentTime: String = getDateMonths(0, "yyyy/MM")
  val transform = new Transform()
  val hbaseReader = new HBaseReader(spark.sparkContext)
  val mysqlReader = new MysqlReader(spark.sqlContext)
  /**
   * 从HBase中读取表ORIGINAL_DATA数据
   */
  def materialQuantityRDD: RDD[MaterialQuantityInfo] = {
    val sql =
      s"""
         |select "item_cd", "item_desc","insert_datetime","quantity"
         |from "ORIGINAL_DATA"
         |where "insert_datetime" >= '${historyTime}'
         |and "insert_datetime" <= '${currentTime}'
         |""".stripMargin
    val mqiRDD: RDD[MaterialQuantityInfo] = hbaseReader.toMaterialQuantityInfoRDD(sql)
    mqiRDD
  }

  /**
   * 从HBase中读取表QUANTITY_STATUS数据
   */
  def materialQuantityStatusRDD: RDD[MaterialQuantityInfo] = {
    val sql =
      s"""
         |select "item_cd", "item_desc","insert_datetime","quantity","status"
         |from "QUANTITY_STATUS"
         |where "insert_datetime" >= '${historyTime}'
         |and "insert_datetime" <= '${currentTime}'
         |""".stripMargin
    val mqsRDD: RDD[MaterialQuantityInfo] = hbaseReader.toMaterialQuantityStatusRDD(sql)
    mqsRDD
  }

  /**
   * 从HBase中读取表"SINGLE_STATUS_COUNT"数据
   */
  def singleStatusCount: RDD[SingleStatusCount] = {
    val sql =
      """
        |select "item_cd","status","count" from "SINGLE_STATUS_COUNT"
        |""".stripMargin
    val value: RDD[SingleStatusCount] = hbaseReader.toSingleStatusCountRDD(sql)
    value
  }

  /**
   * 从HBase中读取表"DOUBLE_STATUS_COUNT"数据
   */
  def doubleStatusCount: RDD[DoubleStatusCount] = {
    val sql =
      """
        |select "item_cd","status_head","status_tail","count" from "DOUBLE_STATUS_COUNT"
        |""".stripMargin
    val value: RDD[DoubleStatusCount] = hbaseReader.toDoubleStatusCountRDD(sql)
    value
  }

  def materialStock(tableName:String, numPartitions:String): List[mutable.HashMap[String, Any]] ={

    val dataFrame = mysqlReader.getQueryData(tableName, numPartitions)
    val jsonDataset: Dataset[String] = dataFrame.toJSON
    //    val rdd: RDD[Row] = dataFrame.rdd
    val jsonStringRDD: RDD[String] = jsonDataset.rdd
    val hashMapRDD: RDD[mutable.HashMap[String, Any]] = jsonStringRDD.map(
      jsonString => {
        val jSONObject = JSON.parseObject(jsonString)
        val hashMap = transform.jSONObjectToHashMap(jSONObject)
        hashMap
      }
    )
    hashMapRDD.collect().toList
  }


}
