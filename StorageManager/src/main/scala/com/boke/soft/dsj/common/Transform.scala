package com.boke.soft.dsj.common

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import java.util
import scala.collection.mutable

/**
 * 类型间转换类函数
 */
class Transform extends Serializable {

  /**
   * 将JSONObject转换成HashMap
   *
   * @param jsonObject
   * @return
   */
  def jSONObjectToHashMap(jsonObject: JSONObject): mutable.HashMap[String, Any] = {

    val hashMap = mutable.HashMap[String, Any]()
    val strings: util.Set[String] = jsonObject.keySet()
    val keysIter = strings.iterator()
    try {
      while (keysIter.hasNext) {
        val key: String = keysIter.next()
        val value: Any = jsonObject.get(key)
        hashMap.put(key, value)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    hashMap
  }

  /**
   * 将DataFrame转换成RDD[mutable.HashMap[String, Any]]
   * @param dataFrame
   * @return
   */
  def dataFrameToHashMapRDD(dataFrame: DataFrame): RDD[mutable.HashMap[String, Any]] = {
    val jsonDataset: Dataset[String] = dataFrame.toJSON
    val jsonStringRDD: RDD[String] = jsonDataset.rdd
    val hashMapRDD: RDD[mutable.HashMap[String, Any]] = jsonStringRDD.map(
      jsonString => {
        val jSONObject = JSON.parseObject(jsonString)
        val hashMap = jSONObjectToHashMap(jSONObject)
        hashMap
      }
    )
    hashMapRDD
  }

}
