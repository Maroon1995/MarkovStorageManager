package com.boke.soft.dsj.common

import scala.collection.mutable.ArrayBuffer


object ProduceStatus {
  /**
   * 获取颗粒度，表示有多少种状态
   *
   * @param maxValues 最大值
   * @return 返回多少状态数量
   */
  def getGraininess(maxValues: Double): (Int, Int) = {
    val maxV = math.ceil(maxValues).toInt //向上取整
    var intervalValue = 0 // 间隔值
    // 根据最大值maxV的取值范围，设置数据间隔intervalValue的取值
    if (maxV <= 1200) {
      intervalValue = 10
    } else if (maxV > 1200 && maxV <= 12000) {
      intervalValue = 100
    } else if (maxV > 12000 && maxV <= 120000) {
      intervalValue = 1000
    } else {
      intervalValue = 10000
    }
    val statusNumber = math.round((maxV / intervalValue).toFloat) // 状态个数
    (intervalValue, statusNumber)
  }

  /**
   * 根据当前物料历史上出库的最大值，为当前出库值映射出库状态
   *
   * @param maxValues: 当前物料历史上出库的最大值
   * @param value: 要映射状态的数值
   * @return
   */
  def getStatus(maxValues: Double, value: Double): (String,Int) = {
    var status: (String,Int) = null
    val tuple = getGraininess(maxValues)
    val intervalValue = tuple._1 // 状态步长
    val statusNumber = tuple._2 // 状态个数
    val baseStatus = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z")
    val wheelNumber = math.ceil(statusNumber / 26).toInt // 循环baseStatus多少轮
    for (i <- 0 to statusNumber) {
      val upper = (i + 1) * intervalValue  // 上限
      val lower = i * intervalValue // 下限
      if (value >= lower && value < upper) {
        for (j <- 0 to wheelNumber) {
          if (i >= j * 26 && i < (j + 1) * 26) {
            status = (baseStatus(i - j * 26) * (j + 1),upper)
          }
        }
      }
    }
    status
  }

  /**
   * 获取出库状态列表
   * @param maxValues ：最大出库值
   * @return
   */
  def getTotalStatusList(maxValues: Double): Array[String] = {
    val tuple = getGraininess(maxValues)
    val statusNumber = tuple._2 // 状态个数
    val statusArr = new ArrayBuffer[String]()
    val baseStatus = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z")
    val wheelNumber = math.ceil(statusNumber / 26).toInt // 循环baseStatus多少轮
    for (i <- 0 to statusNumber) {
      for (j <- 0 to wheelNumber) {
        if (i >= j * 26 && i < (j + 1) * 26) {
          val bs = baseStatus(i - j * 26) * (j + 1)
          statusArr.append(bs)
        }
      }
    }
    statusArr.toArray
  }
}
