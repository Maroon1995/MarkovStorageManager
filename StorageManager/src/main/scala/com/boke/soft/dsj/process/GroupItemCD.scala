package com.boke.soft.dsj.process

import com.boke.soft.dsj.bean.MaterialQuantityInfo
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GroupItemCD {

  /**
   * 获取根据item_cd分组
   * @param sc: spark的运行时环境和上下文对象
   * @return
   */
  def GetGroups(sc: SparkContext): RDD[(String, Iterable[MaterialQuantityInfo])] = {
    // 读取数据：DB数库中读取
    val material_quantity: RDD[String] = sc.textFile("D:\\localdata\\material_transaction.csv").repartition(2)
    // 数据解析并封装成 MaterialQuantityInfo
    val MaterialQuantityRDD: RDD[(String, MaterialQuantityInfo)] = material_quantity.map {
      line => {
        val arrs: Array[String] = line.split(",")
        (arrs(0), MaterialQuantityInfo(arrs(0), arrs(1), arrs(2), arrs(3).toDouble))
      }
    }
    // 数据分组计算出库量态
    val valueGroup: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityRDD.groupByKey() // 分组
    valueGroup
  }
}
