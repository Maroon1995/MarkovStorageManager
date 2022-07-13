package com.boke.soft.dsj.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.boke.soft.dsj.bean.MaterialQuantityInfo
import com.boke.soft.dsj.util.DateUtil.getDateMonths
import com.boke.soft.dsj.util.PhoenixUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ByItemCDGroup {

  /**
   * 获取根据item_cd分组
   *
   * @param sc : spark的运行时环境和上下文对象
   * @return
   */
  def GetGroups(sc: SparkContext): RDD[(String, Iterable[MaterialQuantityInfo])] = {
    // 读取数据：HBase数库中读取
    val historyTime = getDateMonths(66, "yyyy/MM")
    val currentTime = getDateMonths(0, "yyyy/MM")
    val sql =
      s"""
         |select "item_cd", "item_desc","insert_datetime","quantity"
         |from "ORIGINAL_DATA"
         |where "insert_datetime" >= '${historyTime}'
         |and "insert_datetime" <= '${currentTime}'
         |""".stripMargin
    val jsonObjects: List[JSONObject] = PhoenixUtil.queryList(sql)

    val jsonObjectsRDD: RDD[JSONObject] = sc.makeRDD(jsonObjects.toSeq).repartition(2).cache()
    // 数据解析并封装成 MaterialQuantityInfo
    val MaterialQuantityRDD: RDD[MaterialQuantityInfo] = jsonObjectsRDD.map(
      jsonObject => JSON.parseObject(jsonObject.toJSONString, classOf[MaterialQuantityInfo])
    )
    val MaterialQuantityMap: RDD[((String, String, String), Double)] = MaterialQuantityRDD.mapPartitions {
      mqiIter =>
        val mqiList = mqiIter.toList
        val tuples: List[((String, String, String), Double)] = mqiList.map {
          mqi => ((mqi.item_cd, mqi.item_desc, mqi.insert_datetime), mqi.quantity)
        }
        tuples.toIterator
    }
    // 数据聚合分组计算
    val MaterialQuantityReduce = MaterialQuantityMap.reduceByKey(_ + _) // 聚合
    val MaterialQuantityReduceMap: RDD[(String, MaterialQuantityInfo)] = MaterialQuantityReduce.map {
      case ((item_cd, item_desc, datetime), quantity) =>
        (item_cd, MaterialQuantityInfo(item_cd, item_desc, datetime, quantity))
    }
    val valueGroup: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityReduceMap.groupByKey() //分组

    // 补充缺失月份，缺失月份出库为0
    // (1) 补充
    val MaterialQuantityRDDs = valueGroup.mapPartitions {
      mapIter => {
        val mapList = mapIter.toList
        val historyTime = getDateMonths(66, "yyyy/MM")
        val currentTime = getDateMonths(0, "yyyy/MM")
        val dateStringList: List[String] = CreateDateTimeList.DateMonthList(historyTime, currentTime)
        val mapper = mapList.flatMap{
          case (item, mqiIter) =>
            val mqiList:List[MaterialQuantityInfo] = mqiIter.toList
            val mqiListDate: List[MaterialQuantityInfo] = dateStringList.map(
              dateTime => {
                val mqiInit = mqiList.head
                MaterialQuantityInfo(mqiInit.item_cd,mqiInit.item_desc,dateTime,0)
              }
            )
            val infoesList: List[MaterialQuantityInfo] = mqiList.union(mqiListDate) // 合并
            infoesList
        }
        mapper.toIterator
      }
    }
    // (2) 聚合
    val mqiMapRDD = MaterialQuantityRDDs.map(mqi => ((mqi.item_cd, mqi.item_desc, mqi.insert_datetime), mqi.quantity)) //转换结构
    val mqiReduceRDD = mqiMapRDD.reduceByKey(_ + _) //聚合
    val mqiValueRDD = mqiReduceRDD.map {
      case ((item_cd, item_desc, insert_datetime), quantity) => (item_cd,MaterialQuantityInfo(item_cd, item_desc, insert_datetime, quantity))
    }
    // (3)按照物料编码分组
    val MaterialQuantityGroups = mqiValueRDD.groupByKey()
    MaterialQuantityGroups
  }
}