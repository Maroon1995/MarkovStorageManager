package com.boke.soft.common

class Max extends Serializable {

  def getMaxListFloat(list: List[Float]): Float = {
    var max: Float = list.head
    for (i <- 1 until list.length) {
      if (list(i) > max) max = list(i)
    }
    max
  }

  def getMaxListFloat(list: List[Int]): Int = {
    var max: Int = list.head
    for (i <- 1 until list.length) {
      if (list(i) > max) max = list(i)
    }
    max
  }

  def getMaxListFloat(list: List[Long]): Long = {
    var max: Long = list.head
    for (i <- 1 until list.length) {
      if (list(i) > max) max = list(i)
    }
    max
  }

  def getMaxListFloat(list: List[Double]): Double = {
    var max: Double = list.head
    for (i <- 1 until list.length) {
      if (list(i) > max) max = list(i)
    }
    max
  }
}
