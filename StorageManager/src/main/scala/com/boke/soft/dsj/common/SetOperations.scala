package com.boke.soft.dsj.common
import scala.collection.immutable

object SetOperations {
  /**
   * 计算两个列表集合的笛卡尔积组合
   * @param xSet
   * @param ySet
   * @return
   */
  def Cartesian(xSet: List[String], ySet: List[String]): immutable.Seq[(String, String)] = {
    for {x <- xSet; y <- ySet} yield (x, y)
  }

}
