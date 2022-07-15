package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{MaterialQuantityInfo, SingleStatusCount}
import com.boke.soft.dsj.common.{Max, ProduceStatus}
import com.boke.soft.dsj.process.CreateSparkContext
import com.boke.soft.dsj.produce.Produce
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

object StatisticSingleStatusCountAPP {

  def main(args: Array[String]): Unit = {
    // 创建运行环境和上下文环境对象
    val sc = CreateSparkContext.getSC
    // 聚合与分组
    val MaterialQuantityStatus: RDD[MaterialQuantityInfo] = Produce.MaterialQuantityStatusRDD(sc) // 获取物料的出库量和状态数据
    val MaterialQuantityStatusMap: RDD[(String, MaterialQuantityInfo)] = MaterialQuantityStatus.map(mqi => (mqi.item_cd, mqi))
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityStatusMap.groupByKey()

    // 统计每种物料的出库最大值和出库状态的颗粒度(item_cd,maxQuantity,graininess)
    val itemStatusGrainRDD: RDD[((String, String), Int)] = MaterialQuantityGroups.mapPartitions {
      iter => {
        val max = new Max()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter = valueGroupList.flatMap {
          case (item, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = max.getMaxListFloat(infoes.map(_.quantity)) // 计算历史出库最大值
            val graininess = ProduceStatus.getGraininess(maxQuantity)._2 // 计算出库状态颗粒度
            val statusArr: List[String] = ProduceStatus.getTotalStatusList(maxQuantity).toList
            statusArr.map { ele => ((item, ele), graininess) }
        }
        valueGroupIter.toIterator
      }
    }

    // 统计每种状态个数((item,A),2)
    // (1) 统计每种状态个数((item,A),2)
    val valueStatusMap: RDD[((String, String), Int)] = MaterialQuantityGroups.flatMap {
      case (item, valueInter) =>
        val valueMap = valueInter.toList.init.map(mqi => ((item, mqi.status), 1))
        valueMap
    }
    // 合并union RDD：itemStatusGrainRDD union valueStatusReduce
    val valueStatusUnionMap: RDD[((String, String), Int)] = itemStatusGrainRDD.union(valueStatusMap).repartition(3).cache()
    val valueStatusReduce: RDD[((String, String), Int)] = valueStatusUnionMap.reduceByKey(_ + _) // 聚合
    val SingleStatusCountRDD: RDD[SingleStatusCount] = valueStatusReduce.map { case ((item_cd, status), count) => SingleStatusCount(item_cd, status, count) }
    /*-----------------------------------------------------------------------------
    落盘
    */
    import org.apache.phoenix.spark._
    SingleStatusCountRDD.saveToPhoenix(
      "SINGLE_STATUS_COUNT",
      Seq("item_cd","status","count"),
      new Configuration,
      Some("master,centos-oracle,Maroon:2181")
    )

    sc.stop()
  }

}
