package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{MaterialQuantityInfo, StatusMatrix}
import com.boke.soft.dsj.common.SetOperations.Cartesian
import com.boke.soft.dsj.common.{Max, ProduceStatus}
import com.boke.soft.dsj.process.{CreateDateTimeList, CreateSparkContext, ByItemCDGroup}
import com.boke.soft.dsj.util.DateUtil.getDateMonths
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object StorageManagerAPP {

  def main(args: Array[String]): Unit = {
    // 创建运行环境和上下文环境对象
    val sc = CreateSparkContext.GetSC
    // 聚合与分组
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = ByItemCDGroup.GetGroups(sc)
    // 统计每种物料的出库最大值和出库状态的颗粒度(item_cd,maxQuantity,graininess)
    val itemStatusGrainRDD: RDD[((String, String, String), Int)] = MaterialQuantityGroups.mapPartitions {
      iter => {
        val max = new Max()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter = valueGroupList.flatMap {
          case (item, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = max.getMaxListFloat(infoes.map(_.quantity)) // 计算历史出库最大值
            val graininess = ProduceStatus.GetGraininess(maxQuantity)._2 // 计算出库状态颗粒度
            val statusArr: List[String] = ProduceStatus.GetTotalStatusList(maxQuantity).toList
            statusArr.map { ele => ((item, ele, "1"), graininess) }
        }
        valueGroupIter.toIterator
      }
    }

    val itemStatusGrainTwoRDD = MaterialQuantityGroups.mapPartitions {
      iter => {
        val max = new Max()
        val valueGroupList: List[(String, Iterable[MaterialQuantityInfo])] = iter.toList
        val valueGroupIter = valueGroupList.flatMap {
          case (item, iter) =>
            val infoes: List[MaterialQuantityInfo] = iter.toList
            val maxQuantity = max.getMaxListFloat(infoes.map(_.quantity)) // 计算历史出库最大值
            val statusArr: List[String] = ProduceStatus.GetTotalStatusList(maxQuantity).toList
            val statusCartensian: List[(String, String)] = Cartesian(statusArr, statusArr).toList
            statusCartensian.map { ele => ((item, ele._1, ele._2), 1) }
        }
        valueGroupIter.toIterator
      }
    }

    /* --------------------------------------------------------------------------------------------
    根据分组计算物料出库量所属出库状态
     */
    val valueStatus: RDD[(String, Iterator[MaterialQuantityInfo])] = MaterialQuantityGroups.mapPartitions {
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
    // 统计每种状态个数((item,A),2)和每个相邻状态个数(((item,A),B),2))
    // (1) 统计每种状态个数((item,A),2)
    val valueStatusMap: RDD[((String, String, String), Int)] = valueStatus.flatMap {
      case (item, valueInter) =>
        val valueMap = valueInter.toList.init.map(mqi => ((item, mqi.status, "1"), 1))
        valueMap
    }
    // 合并union RDD：itemStatusGrainRDD union valueStatusReduce
    val valueStatusUnionMap: RDD[((String, String, String), Int)] = itemStatusGrainRDD.union(valueStatusMap).repartition(3).cache()
    val valueStatusReduce: RDD[((String, String, String), Int)] = valueStatusUnionMap.reduceByKey(_ + _) // 聚合

    // (2) 统计相邻状态个数(((item,A),B),2))
    val valueStatusMapTwo = valueStatus.flatMap {
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

    // (3) 计算概率
    //    val valueStatusUnionReduce: RDD[((String, String, String), Int)] = valueStatusReduceTwo.union(valueStatusReduce)
    //    valueStatusUnionReduce.map{
    //      case ((item, status1, status2), count) => {
    //
    //      }
    //    }
    /*-----------------------------------------------------------------------------
    打印区
    */
    //    valueStatusReduce.foreach {
    //      case ((item, status1,ng), count) => println(item + " " + status1 + " " + ng +" " + count)
    //    }
    //    valueStatusReduceTwo.foreach {
    //      case ((item, status1, status2), count) => println(item + " " + status1 + " " + status2 + " " + count)
    //    }

    MaterialQuantityGroups.foreach {
      case (item, mqiIterable) => {
        for (mqi <- mqiIterable) {
          println(item + ": " + mqi.toString)
        }
      }
    }
    // 关闭资源
    sc.stop()
  }
}
