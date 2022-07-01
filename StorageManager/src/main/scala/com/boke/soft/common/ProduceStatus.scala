package com.boke.soft.common

object ProduceStatus {

  def GetStatus(maxValues: Double, value: Double): String = {
    val maxV = math.ceil(maxValues).toInt //向上取整
    var intervalValue = 0 // 间隔值
    var status: String = null
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
    val baseStatus = Array("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z")
    val statusNumber = math.round((maxV / intervalValue).toFloat) // 状态个数
    val wheelNumber = math.ceil(statusNumber / 26).toInt // 循环baseStatus多少论
    for (i <- 0 to statusNumber) {
      if (value >= i * intervalValue && value < (i + 1) * intervalValue) {
        for (j <- 0 to wheelNumber) {
          if (i >= j * 26 && i < (j + 1) * 26) {
            status = baseStatus(i - j * 26) * (j + 1)
          }
        }
      }
    }
    status
  }

  def main(args: Array[String]): Unit = {

    println(GetStatus(1000.1.toFloat, 1000))
  }
}
