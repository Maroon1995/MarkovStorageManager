package com.boke.soft.dsj.io

import com.boke.soft.dsj.bean.{DoubleStatusCount, MaterialQuantityInfo, SingleStatusCount}
import com.boke.soft.dsj.util.PhoenixUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

class HBaseReader(sc: SparkContext) extends Serializable {

  /**
   * RDD重分区数
   */
  var numPartition: Int = 4

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
  def toMaterialQuantityInfoRDD(sql: String): RDD[MaterialQuantityInfo] = {
    val MaterialQuantityInfoList = toMaterialQuantityInfoList(sql)
    val MaterialQuantityInfoRDD: RDD[MaterialQuantityInfo] = sc.makeRDD(MaterialQuantityInfoList.toSeq).repartition(numPartition)
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
        resultSet.getObject(5).toString,
        resultSet.getObject(6, classOf[Long])
      )
      listBufferMQI.append(mqi)
    }
    // 释放资源
    PhoenixUtil.close()
    listBufferMQI.toList
  }

  def toMaterialQuantityStatusRDD(sql: String): RDD[MaterialQuantityInfo] = {
    val MaterialQuantityStatusList = toMaterialQuantityStatusList(sql)
    val MaterialQuantityStatusRDD: RDD[MaterialQuantityInfo] = sc.makeRDD(MaterialQuantityStatusList.toSeq).repartition(numPartition)
    MaterialQuantityStatusRDD
  }

  def toDoubleStatusCountRDD(sql: String): RDD[DoubleStatusCount] = {
    val resultSet = PhoenixUtil.queryToResultSet(sql)
    // 处理结果集
    val listBufferDSC = new ListBuffer[DoubleStatusCount]()
    while (resultSet.next()) {
      // 将结果封装成MaterialQuantityInfo对象
      val dsc: DoubleStatusCount = DoubleStatusCount(
        resultSet.getObject(1).toString,
        resultSet.getObject(2).toString,
        resultSet.getObject(3).toString,
        resultSet.getObject(4, classOf[Double])
      )
      listBufferDSC.append(dsc)
    }
    // 释放资源
    PhoenixUtil.close()
    val doubleStatusCountRDD: RDD[DoubleStatusCount] = sc.makeRDD(listBufferDSC.toSeq).repartition(numPartition)()
    doubleStatusCountRDD
  }

  def toSingleStatusCountRDD(sql: String): RDD[SingleStatusCount] = {
    val resultSet = PhoenixUtil.queryToResultSet(sql)
    // 处理结果集
    val listBufferSSC = new ListBuffer[SingleStatusCount]()
    while (resultSet.next()) {
      // 将结果封装成MaterialQuantityInfo对象
      val ssc: SingleStatusCount = SingleStatusCount(
        resultSet.getObject(1).toString,
        resultSet.getObject(2).toString,
        resultSet.getObject(3, classOf[Double])
      )
      listBufferSSC.append(ssc)
    }
    // 释放资源
    PhoenixUtil.close()
    val singleStatusCountRDD: RDD[SingleStatusCount] = sc.makeRDD(listBufferSSC.toSeq).repartition(numPartition)()
    singleStatusCountRDD
  }
}
