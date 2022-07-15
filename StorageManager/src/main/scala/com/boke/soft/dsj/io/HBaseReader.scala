package com.boke.soft.dsj.io

import com.alibaba.fastjson.JSONObject
import com.boke.soft.dsj.bean.MaterialQuantityInfo
import com.boke.soft.dsj.util.PhoenixUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

class HBaseReader(sc: SparkContext) extends Serializable {

  /**
   * RDD重分区数
   */
  var numPartition: Int = 4

  /**
   * 将查询结果转换成ResultSet
   */
  def toResultSet(sql: String): ResultSet = {
    val resultSet = PhoenixUtil.queryToResultSet(sql)
    PhoenixUtil.close()
    resultSet
  }
  /**
   * 将查询结果转换成JSONObject列表
   */
  def toJSONObjectList(sql: String): List[JSONObject] = {
    val jSONObjects = PhoenixUtil.queryToJSONObjectList(sql)
    jSONObjects
  }

  /**
   * 将查询结果转换成JSONObject的RDD
   */
  def toJSONObjectRDD(sql: String): RDD[JSONObject] = {
    val jSONObjects = PhoenixUtil.queryToJSONObjectList(sql)
    val jsonObjectsRDD: RDD[JSONObject] = sc.makeRDD(jSONObjects.toSeq).repartition(numPartition).cache()
    jsonObjectsRDD
  }

  /**
   * 将查询结果转封装成MaterialQuantityInfo列表
   */
  def toMaterialQuantityInfoList(sql: String): List[MaterialQuantityInfo] = {
    val resultSet = PhoenixUtil.queryToResultSet(sql)
    // 处理结果集
    val listBufferMQI = new ListBuffer[MaterialQuantityInfo]()
    while (resultSet.next()) {
      // 将结果封装成MaterialQuantityInfo对象
      val mqi: MaterialQuantityInfo = MaterialQuantityInfo(
        resultSet.getObject(1).toString,
        resultSet.getObject(2).toString,
        resultSet.getObject(3).toString,
        resultSet.getObject(4, classOf[Double])
      )
      listBufferMQI.append(mqi)
    }
    // 释放资源
    PhoenixUtil.close()
    listBufferMQI.toList
  }

  /**
   * 将查询结果转封装成MaterialQuantityInfo的RDD
   */
  def toMaterialQuantityInfoRDD(sql: String):RDD[MaterialQuantityInfo] ={
    val MaterialQuantityInfoList = toMaterialQuantityInfoList(sql)
    val MaterialQuantityInfoRDD: RDD[MaterialQuantityInfo] = sc.makeRDD(MaterialQuantityInfoList.toSeq).repartition(numPartition).cache()
    MaterialQuantityInfoRDD
  }

  def toMaterialQuantityStatusList(sql: String): List[MaterialQuantityInfo] = {
    val resultSet = PhoenixUtil.queryToResultSet(sql)
    // 处理结果集
    val listBufferMQI = new ListBuffer[MaterialQuantityInfo]()
    while (resultSet.next()) {
      // 将结果封装成MaterialQuantityInfo对象
      val mqi: MaterialQuantityInfo = MaterialQuantityInfo(
        resultSet.getObject(1).toString,
        resultSet.getObject(2).toString,
        resultSet.getObject(3).toString,
        resultSet.getObject(4, classOf[Double]),
        resultSet.getObject(5).toString
      )
      listBufferMQI.append(mqi)
    }
    // 释放资源
    PhoenixUtil.close()
    listBufferMQI.toList
  }

  def toMaterialQuantityStatusRDD(sql:String):RDD[MaterialQuantityInfo]={
    val MaterialQuantityStatusList = toMaterialQuantityStatusList(sql)
    val MaterialQuantityStatusRDD: RDD[MaterialQuantityInfo] = sc.makeRDD(MaterialQuantityStatusList.toSeq).repartition(numPartition).cache()
    MaterialQuantityStatusRDD
  }
}
