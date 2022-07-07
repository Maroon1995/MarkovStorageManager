package com.boke.soft.dsj.process

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

class DefAccumulator extends AccumulatorV2[((String, String, String), Double), mutable.Map[(String, String, String), Double]] {
  var mapAcc: mutable.Map[(String, String, String), Double] = mutable.Map()

  // 判断累加器是不是初始值
  override def isZero: Boolean = mapAcc.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[((String, String, String), Double), mutable.Map[(String, String, String), Double]] = new DefAccumulator

  // 清空累加器
  override def reset(): Unit = mapAcc.clear()

  // 累加器累加逻辑
  override def add(v: ((String, String, String), Double)): Unit = {

    mapAcc.update(v._1, mapAcc.getOrElse(v._1, 0.toDouble) + v._2)
  }

  // 区间合并
  override def merge(other: AccumulatorV2[((String, String, String), Double), mutable.Map[(String, String, String), Double]]): Unit = {
    val map2 = other.value
    val map1 = mapAcc
    mapAcc = map1.foldLeft(map2) {
      case (innerMap, kv) =>
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0.toDouble) + kv._2
        innerMap
    }
  }
  // 获取最终的累加值
  override def value: mutable.Map[(String, String, String), Double] = mapAcc
}
