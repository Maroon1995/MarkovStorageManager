package com.boke.soft.dsj.common

import com.alibaba.fastjson.JSONObject

import java.util
import scala.collection.mutable

/**
 * 类型间转换类函数
 */
class Transform extends Serializable {

  /**
   * 将JSONObject转换成HashMap
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

}
