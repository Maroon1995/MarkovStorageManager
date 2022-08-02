package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.MaterialQuantityInfo
import com.boke.soft.dsj.common.{MyMath, ProduceStatus}
import com.boke.soft.dsj.process.{ByItemCDGroup, CreateSparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

object CaculateQuantityStatusAPP {

  def main(args: Array[String]): Unit = {
    // 创建运行环境和上下文环境对象
    val sc = CreateSparkContext.getSC("CaculateQuantityStatus")
    // 聚合与分组
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = ByItemCDGroup.getGroups(sc)
    // 根据分组计算物料出库量所属出库状态
    val quantityStatus = MaterialQuantityGroups.mapPartitions {
      iter => {
        val math = new MyMath()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter = valueGroupList.flatMap {
          case (key, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = math.getMaxFromList[Double](infoes.map(_.quantity)) // 计算历史出库最大值
            val quantityInfoes = infoes.map { // 计算出库量所属状态
              mqi => {
                val value = mqi.quantity
                val status = ProduceStatus.getStatus(maxQuantity, value)
                mqi.status = status._1 // 状态码
                mqi.upper = status._2 // 状态上线值
                mqi
              }
            }
            quantityInfoes.toIterator
        }.toIterator
        valueGroupIter
      }
    }

    // 落盘
    import org.apache.phoenix.spark._
    quantityStatus.saveToPhoenix(
      "QUANTITY_STATUS",
      Seq("item_cd","item_desc","insert_datetime","quantity","status"),
      new Configuration,
      Some("master,centos-oracle,Maroon:2181")
    )
    // 关闭资源
    sc.stop()
  }
}
