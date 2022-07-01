package com.boke.soft.app

import com.boke.soft.bean.MaterialQuantityInfo
import com.boke.soft.common.ProduceStatus._
import com.boke.soft.common.{Max, ProduceStatus}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object StorageManagerAPP {

  def main(args: Array[String]): Unit = {
    // 创建运行环境和上下文环境对象
    val conf = new SparkConf().setMaster("local[4]").setAppName("storagemanager")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("H:\\Project\\scalaworkspace\\StorageManagerSystem\\StorageManager\\src\\main\\checkpoint")
    // 读取数据
    val material_quantity: RDD[String] = sc.textFile("D:\\localdata\\material_transaction.csv").repartition(2)
    // 数据解析封装MaterialQuantityInfo
    val MaterialQuantityRDD: RDD[(String,MaterialQuantityInfo)] = material_quantity.map {
      line => {
        val arrs: Array[String] = line.split(",")
        (arrs(0),MaterialQuantityInfo(arrs(0), arrs(1), arrs(2), arrs(3).toDouble))
      }
    }.cache()
    // 数据分组计算出库量态
    val valueGroup: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityRDD.groupByKey()
    // 根据分组计算物料出库量所属出库状态
    val valueStatus: RDD[(String, List[MaterialQuantityInfo])] = valueGroup.mapPartitions {
      iter => {
        val max = new Max()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter: Iterator[(String, List[MaterialQuantityInfo])] = valueGroupList.map {
          case (key, iter) =>
//            val infoes: List[MaterialQuantityInfo] = iter.toList.sortWith(_.quantity < _.quantity)
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = max.getMaxListFloat(infoes.map(_.quantity)) // 计算历史出库最大值

            val quantityInfoes = infoes.map { // 计算出库量所属状态
              mqi => {
                val value = mqi.quantity
                mqi.status = ProduceStatus.GetStatus(maxQuantity, value)
                mqi
              }
            }
            (key, quantityInfoes)
        }.toIterator
        valueGroupIter
      }
    }

    valueStatus.foreach(println)
    // 关闭资源
    sc.stop()
  }

}
