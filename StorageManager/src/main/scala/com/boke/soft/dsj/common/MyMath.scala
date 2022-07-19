package com.boke.soft.dsj.common




class MyMath extends Serializable {

  /**
   * 获取列表中数据的最大值
   */

  def getMaxFromList[T <% Ordered[T]](list:List[T]): T ={
    var max: T = list.head
    for (i <- 1 until list.length) {
      if (list(i) > max) max = list(i)
    }
    max
  }

  /**
   * 获取小数点或的第n位
   */
  def round(value: Double, n: Int): Double = {
    val str: String = value.formatted(s"%.${n}f")
    str.toDouble
  }

}
