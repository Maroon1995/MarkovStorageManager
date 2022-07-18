package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{DoubleStatusCount, SingleStatusCount, StatusMatrix}
import com.boke.soft.dsj.process.CreateSparkContext
import com.boke.soft.dsj.produce.Produce
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CaculateStatusMatrixAPP {

  def main(args: Array[String]): Unit = {

    // TODO 0 创建运行环境和上下文环境对象
    val sc = CreateSparkContext.getSC
    sc.setCheckpointDir("H:\\Project\\scalaworkspace\\StorageManagerSystem\\StorageManager\\src\\main\\checkpoint\\")
    val produce = new Produce(sc)
    // TODO 1 读取DoubleStatusCount和SingleStatusCount数据
    val singleStatusCount: RDD[SingleStatusCount] = produce.singleStatusCount.cache()
    val doubleStatusCount: RDD[DoubleStatusCount] = produce.doubleStatusCount.cache()
    // TODO 2 计算概率，并封装成状态概率矩阵：StatusMatrix
    val singleStatusCountMapRDD: RDD[mutable.HashMap[String, (String, Double)]] = singleStatusCount.map {
      ssc =>
        val sscMap: mutable.HashMap[String, (String, Double)] = mutable.HashMap[String, (String, Double)]()
        sscMap.put(ssc.item_cd, (ssc.status, ssc.count))
        sscMap
    }

    val doubleStatusCountMapRDD: RDD[mutable.HashMap[String, mutable.HashMap[String, (String, Double)]]] = doubleStatusCount.map {
      dsc =>
        val stringToTuple: mutable.HashMap[String, (String, Double)] = mutable.HashMap[String, (String, Double)]()
        val dscMap: mutable.HashMap[String, mutable.HashMap[String, (String, Double)]] = mutable.HashMap[String, mutable.HashMap[String, (String, Double)]]()
        stringToTuple.put(dsc.status_head, (dsc.status_tail, dsc.count))
        dscMap.put(dsc.item_cd, stringToTuple)
        dscMap
    }
    doubleStatusCountMapRDD.map {
      mapper => {
        var matrix: StatusMatrix = null
        singleStatusCount.foreach{
          ssc => {
            val tuple: (String, Double) = mapper(ssc.item_cd)(ssc.status)
            if (tuple != null) {
              matrix = StatusMatrix(ssc.item_cd, ssc.status, tuple._1, tuple._2 / ssc.count)
            }
          }
        }
        matrix
      }
    }


    singleStatusCount.foreach(println)
    doubleStatusCount.foreach(println)
    // TODO 关闭环境
    sc.stop()
  }

}
