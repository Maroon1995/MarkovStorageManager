package com.boke.soft.dsj.util

import java.util.Date
import org.joda.time.{DateTime, Months}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.collection.mutable.ListBuffer

/**
 * 日期时间工具类
 *
 * Joda实现
 * create by L 2020/7/12
 */
object DateUtil {
  val DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val TIME_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")
  val DATE_KEY_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  val TIME_MINUTE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm")


  /**
   * 获取昨天的, 格式: yyyy-MM-dd
   *
   * @return 昨天的日期
   */
  def getYesterdayDate: String = {
    DateTime.now().minusDays(1).toString(DATE_FORMAT)
  }

  /**
   * 获取当天日期, 格式: yyyy-MM-dd
   *
   * @return 当天日期
   */
  def getTodayDate: String = {
    DateTime.now().toString(DATE_FORMAT)
  }

  /**
   * 获取距当天 N 天时的日期 格式: yyyy-MM-dd
   *
   * @param n : 当天为 0，前一天为 1, 后一天为 -1
   * @return yyyy-MM-dd
   */
  def getDateDays(n: Int): String = {
    DateTime.now().minusDays(n).toString(DATE_FORMAT)
  }

  /**
   * 获取距当月 N 月时的日期 格式: format
   *
   * @param n      : 当月为 0，前一月为 1, 后一月为 -1
   * @param format : 日期格式 "yyyy/MM"
   * @return
   */
  def getDateMonths(n: Int, format: String): String = {
    DateTime.now().minusMonths(n).toString(format)
  }

  /**
   * 获取日期时间 格式: yyyy-MM-dd
   *
   * @param yesterday : 需要计算的日期
   * @param n         : 当天为 0，前一天为 1, 后一天为 -1
   * @return yyyy-MM-dd
   */
  def getOneDate(yesterday: String, n: Int): String = {
    DateTime.parse(yesterday).minusDays(n).toString(DATE_FORMAT)
  }

  def getNowTime: String = {
    DateTime.now().toString(TIME_FORMAT)
  }

  /**
   * 格式化日期, 格式: yyyy-MM-dd
   *
   * @param date Date对象
   * @return 格式化后的日期
   */
  def formatDate(date: Date): String = {
    new DateTime(date).toString(DATE_FORMAT)
  }

  /**
   * 格式化时间, 格式: yyyy-MM-dd HH:mm:ss
   *
   * @param date Date对象
   * @return 格式化后的时间
   */
  def formatTime(date: Date): String = {
    new DateTime(date).toString(TIME_FORMAT)
  }

  /**
   * 格式化日期key, 格式: yyyyMMdd
   *
   * @param date : Date对象, Sat Sep 07 03:02:01 CST 2019
   * @return yyyyMMdd
   */
  def formatDateKey(date: Date): String = {
    new DateTime(date).toString(DATE_KEY_FORMAT)
  }

  /**
   * 格式化日期key, 格式: yyyyMMdd
   *
   * @param date : time String, yyyy-MM-dd HH:mm:ss
   * @return yyyyMMdd
   */
  def formatDateKey(date: String): Date = {
    DATE_KEY_FORMAT.parseDateTime(date).toDate
  }

  /**
   * 格式化日期key, 格式: yyyyMMdd
   *
   * @param date : time String, yyyy-MM-dd HH:mm:ss
   * @return yyyyMMdd
   */
  def dateToTimestamp(date: String): Long = {
    TIME_FORMAT.parseDateTime(date).getMillis
  }

  /**
   * 格式化日期为0点, 格式: yyyy-MM-dd 00:00:00
   *
   * @param date : time String, yyyy-MM-dd HH:mm:ss
   * @return yyyy-MM-dd 00:00:00
   */
  def formatTimeZone(date: String): String = {
    TIME_FORMAT.parseDateTime(date).toString("yyyy-MM-dd 00:00:00")
  }

  /**
   * 时间戳转日期, 格式: yyyy-MM-dd
   *
   * @param timestamp : 时间戳, Long
   * @return yyyy-MM-dd
   */
  def timestampToDate(timestamp: Long): String = {
    new DateTime(timestamp).toString(DATE_FORMAT)
  }

  /**
   * 时间戳转time, 格式: yyyy-MM-dd HH:mm:ss
   *
   * @param timestamp : 时间戳, Long, 13 位
   * @return yyyy-MM-dd HH:mm:ss
   */
  def timestampToTime(timestamp: Long): String = {
    new DateTime(timestamp).toString(TIME_FORMAT)
  }

  /**
   * 时间戳转指定格式time, 格式: 自定义
   *
   * @param timestamp : 时间戳, Long, 13 位
   * @param pattern   : 格式化格式,如: yyyy-MM-dd
   * @return 自定义
   */
  def timestampToTime(timestamp: Long, pattern: String): String = {
    new DateTime(timestamp).toString(pattern)
  }

  /**
   * 通过时间戳获取小时, 格式: HH
   *
   * @param timestamp : 时间戳, Long, 13 位
   * @return 小时, Int
   */
  def getHour(timestamp: Long): Int = {
    new DateTime(timestamp).hourOfDay().getAsString.toInt
  }

  /**
   * 通过时间戳获取分钟, 格式: mm
   *
   * @param timestamp : 时间戳, Long, 13 位
   * @return 分钟, Int
   */
  def getMinute(timestamp: Long): Int = {
    new DateTime(timestamp).millisOfSecond().getAsString.toInt
  }

  /**
   * 通过时间戳获取秒, 格式: ss
   *
   * @param timestamp : 时间戳, Long, 13 位
   * @return 秒, Int
   */
  def getSecond(timestamp: Long): Int = {
    new DateTime(timestamp).secondOfMinute().getAsString.toInt
  }

  /**
   * 将时间转换为日期格式,格式化到月
   *
   * @param date yyyy-MM-dd HH:mm:ss
   * @return yyyy-MM
   */
  def formatDateToMonth(date: String): String = {
    TIME_FORMAT.parseDateTime(date).toString("yyyy/MM")
  }

  /**
   * 计算两个日期的差值
   *
   * @param dateTime1 :字符串日期1
   * @param dateTime2 :字符串日期2
   * @return 返回相差的月数
   */
  def getMonthDifferent(dateTime1: DateTime, dateTime2: DateTime): Int = {
    Months.monthsBetween(dateTime1, dateTime2).getMonths
  }

  /**
   * 计算两个日期的差值
   *
   * @param dateTime1 :字符串小日期1
   * @param dateTime2 :字符串大日期2
   * @param format    : 输入字符串日期的解析格式
   * @return 返回相差的月数
   */
  def getMonthDifferent(dateTime1: String, dateTime2: String, format: String): Int = {
    val formatter = DateTimeFormat.forPattern(format)
    val date1 = formatter.parseDateTime(dateTime1)
    val date2 = formatter.parseDateTime(dateTime2)
    Months.monthsBetween(date1, date2).getMonths
  }

  def main(args: Array[String]): Unit = {
    val date1 = "2018/9"
    val date2 = "2017/9"
    //    println(getDateMonths(2, "yyyy/MM"))
    val diffMonthsNumber = getMonthDifferent(date2, date1, "yyyy/MM")
    val dateList: ListBuffer[String] = ListBuffer[String]()
    for (i <- 0 to diffMonthsNumber) {
      dateList.append(getDateMonths(i,"yyyy/MM"))
    }
    dateList
  }
}
