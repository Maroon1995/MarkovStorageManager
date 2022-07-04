package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.MaterialQuantityInfo
import com.boke.soft.dsj.common.{Max, ProduceStatus}
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
    // 数据解析并封装成 MaterialQuantityInfo
    val MaterialQuantityRDD: RDD[(String, MaterialQuantityInfo)] = material_quantity.map {
      line => {
        val arrs: Array[String] = line.split(",")
        (arrs(0), MaterialQuantityInfo(arrs(0), arrs(1), arrs(2), arrs(3).toDouble))
      }
    }
    // 数据分组计算出库量态
    val valueGroup: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityRDD.groupByKey() // 分组
    // 统计每种物料的出库最大值和出库状态的颗粒度(item_cd,maxQuantity,graininess)。
    val itemCdInfoRDD = valueGroup.mapPartitions {
      iter => {
        val max = new Max()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter = valueGroupList.flatMap {
          case (key, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = max.getMaxListFloat(infoes.map(_.quantity)) // 计算历史出库最大值
            val graininess = ProduceStatus.GetGraininess(maxQuantity)._2 // 计算出库状态颗粒度
            val statusArr: List[String] = ProduceStatus.GetTotalStatusList(maxQuantity).toList
            statusArr.map((key, _, graininess))
        }
        valueGroupIter.toIterator
      }
    }
    // --------------------------------------------------------------------------------------------
    // 根据分组计算物料出库量所属出库状态
    val valueStatus: RDD[(String, Iterator[MaterialQuantityInfo])] = valueGroup.mapPartitions {
      iter => {
        val max = new Max()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter: Iterator[(String, Iterator[MaterialQuantityInfo])] = valueGroupList.map {
          case (key, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = max.getMaxListFloat(infoes.map(_.quantity)) // 计算历史出库最大值
            val quantityInfoes = infoes.map { // 计算出库量所属状态
              mqi => {
                val value = mqi.quantity
                mqi.status = ProduceStatus.GetStatus(maxQuantity, value)
                mqi
              }
            }
            (key, quantityInfoes.toIterator)
        }.toIterator
        valueGroupIter
      }
    }.cache()
    // 统计每种状态个数(A,2)和每个相邻状态个数((A,B),2)
    // (1) 统计每种状态个数
    val valueStatusMap: RDD[((String, String), Int)] = valueStatus.flatMap {
      case (item, valueInter) =>
        val valueMap = valueInter.map(mqi => ((item, mqi.status), 1))
        valueMap
    }
    val valueStatusReduce: RDD[((String, String), Int)] = valueStatusMap.reduceByKey(_ + _)
    valueStatusReduce.map {
      case ((item, status), count) =>

    }
    valueStatusReduce.foreach(println)
    // 关闭资源
    sc.stop()
  }
}
