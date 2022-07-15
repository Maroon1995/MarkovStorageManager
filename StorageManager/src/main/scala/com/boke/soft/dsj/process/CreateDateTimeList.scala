package com.boke.soft.dsj.process

import com.boke.soft.dsj.util.DateUtil.{getDateMonths, getMonthDifferent}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object CreateDateTimeList {

  /**
   * 获取间隔时间为月的日期列表
   * @param startDate: 开始日期
   * @param endDate： 结束日期
   * @return 返回日期列表，将时间为月
   */
  def dateMonthList(startDate: String, endDate: String): List[String] = {
    val diffMonthsNumber = getMonthDifferent(startDate, endDate, "yyyy/MM") // 计算日期差值
    val dateList: ListBuffer[String] = ListBuffer[String]()
    for (i <- 0 to diffMonthsNumber) {
      dateList.append(getDateMonths(i, "yyyy/MM"))
    }
    dateList.toList
  }

  /**
   * 获取间隔时间为月的日期列表
   * @param startDate: 开始日期
   * @param endDate： 结束日期
   * @return 返回日期列表，将时间为月
   */
  def dateMonthList(startDate: DateTime, endDate: DateTime): List[String] ={
    val diffMonthsNumber = getMonthDifferent(endDate, startDate) // 计算日期差值
    val dateList: ListBuffer[String] = ListBuffer[String]()
    for (i <- 0 to diffMonthsNumber) {
      dateList.append(getDateMonths(i, "yyyy/MM"))
    }
    dateList.toList
  }
}
