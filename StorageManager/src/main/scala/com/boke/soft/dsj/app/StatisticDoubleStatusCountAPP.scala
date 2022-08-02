package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{DoubleStatusCount, MaterialQuantityInfo}
import com.boke.soft.dsj.common.{MyMath, ProduceStatus}
import com.boke.soft.dsj.common.SetOperations.Cartesian
import com.boke.soft.dsj.process.CreateSparkContext
import com.boke.soft.dsj.produce.Produce
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object StatisticDoubleStatusCountAPP {

  def main(args: Array[String]): Unit = {
    // 创建运行环境和上下文环境对象
    val sc = CreateSparkContext.getSC("StatisticDoubleStatusCount")
    // 获取数据
    val produce = new Produce(sc)
    val MaterialQuantityStatus:RDD[MaterialQuantityInfo] = produce.materialQuantityStatusRDD // 获取物料的出库量和状态数据
    val MaterialQuantityStatusMap: RDD[(String, MaterialQuantityInfo)] = MaterialQuantityStatus.map(mqi => (mqi.item_cd, mqi))
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityStatusMap.groupByKey()

    // 统计每种物料的出库最大值和有多少种出库状态个数(item_cd,maxQuantity,graininess)
    val itemStatusGrainTwoRDD = MaterialQuantityGroups.mapPartitions {
      iter => {
        val math = new MyMath()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter = valueGroupList.flatMap {
          case (item, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = math.getMaxFromList[Double](infoes.map(_.quantity)) // 计算历史出库最大值
            val statusArr: List[String] = ProduceStatus.getTotalStatusList(maxQuantity).toList
            val statusCartensian: List[(String, String)] = Cartesian(statusArr, statusArr).toList
            statusCartensian.map { ele => ((item, ele._1, ele._2), 1) }
        }
        valueGroupIter.toIterator
      }
    }

    // 每个相邻状态个数(((item,A),B),2))
    val valueStatusMapTwo = MaterialQuantityGroups.flatMap {
      case (item, valueInter) =>
        val valueList = valueInter.toList
        val valueListBuffer: ListBuffer[((String, String, String), Int)] = new ListBuffer[((String, String, String), Int)]()
        for (i <- 0 until (valueList.length - 1)) {
          valueListBuffer.append(((item, valueList(i).status, valueList(i + 1).status), 1))
        }
        valueListBuffer.toIterator
    }
    // 合并 union RDD：itemStatusGrainTwoRDD union valueStatusMapTwo
    val valueStatusUnionMapTwo: RDD[((String, String, String), Int)] = itemStatusGrainTwoRDD.union(valueStatusMapTwo).repartition(3).cache()
    val valueStatusReduceTwo: RDD[((String, String, String), Int)] = valueStatusUnionMapTwo.reduceByKey(_ + _)
    val DoubleStatusCountRDD: RDD[DoubleStatusCount] = valueStatusReduceTwo.map {
      case ((item_cd, status_head, status_tail), count) =>
        DoubleStatusCount(item_cd, status_head, status_tail, count)
    }
    /*-----------------------------------------------------------------------------
    落盘
    */
    import org.apache.phoenix.spark._
    DoubleStatusCountRDD.saveToPhoenix(
      "DOUBLE_STATUS_COUNT",
      Seq("item_cd","status_head","status_tail","count"),
      new Configuration,
      Some("master,centos-oracle,Maroon:2181")
    )

    // 关闭资源
    sc.stop()
  }

}
