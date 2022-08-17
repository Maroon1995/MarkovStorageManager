package com.boke.soft.dsj.app

import org.apache.spark.rdd.RDD
import com.boke.soft.dsj.common.MyMath
import com.boke.soft.dsj.produce.Produce
import org.apache.hadoop.conf.Configuration
import com.boke.soft.dsj.process.CreateSpark
import com.boke.soft.dsj.bean.{DoubleStatusCount, SingleStatusCount, StatusMatrix}

object CaculateStatusMatrixAPP {

  def main(args: Array[String]): Unit = {

    // TODO 0 创建运行环境和上下文环境对象
    val spark = CreateSpark.getSpark("CaculateStatusMatrix")
    val produce = new Produce(spark)
    // TODO 1 读取DoubleStatusCount和SingleStatusCount数据
    val singleStatusCount: RDD[SingleStatusCount] = produce.singleStatusCount.cache()
    val doubleStatusCount: RDD[DoubleStatusCount] = produce.doubleStatusCount.cache()
    // TODO 2 计算概率，并封装成状态概率矩阵：StatusMatrix
    // Mapper
    val singleStatusCountMapRDD = singleStatusCount.map {
      ssc =>
        ((ssc.item_cd, ssc.status), ("1", ssc.count))
    }
    val doubleStatusCountMapRDD = doubleStatusCount.map {
      dsc =>
        ((dsc.item_cd, dsc.status_head), (dsc.status_tail, dsc.count))
    }
    // Combine
    val value = doubleStatusCountMapRDD.union(singleStatusCountMapRDD).cache()
    // Group
    val valueGroup = value.groupByKey()
    val statusMatrixRDD: RDD[StatusMatrix] = valueGroup.mapPartitions(
      tupleIter => {
        val math = new MyMath()
        val tupleList: List[((String, String), Iterable[(String, Double)])] = tupleIter.toList
        val matrixesList: List[StatusMatrix] = tupleList.flatMap {
          case (key, mapIter) =>
            val mapList: List[(String, Double)] = mapIter.toList
            val doubleStatus: List[(String, Double)] = mapList.filter(mapper => mapper._1 != "1")
            val singleStatus: (String, Double) = mapList.filter(mapper => mapper._1 == "1").head
            val matrixes: List[StatusMatrix] = doubleStatus.map(
              mapper =>
                StatusMatrix(key._1, key._2, mapper._1, math.round(mapper._2 / singleStatus._2, 3))
            )
            matrixes
        }
        matrixesList.toIterator
      }
    )
    // TODO 3 将结果保存到hbase
    import org.apache.phoenix.spark._
    statusMatrixRDD.saveToPhoenix(
      "STATUS_MATRIX",
      Seq("item_cd","xAxis","yAxis","probability"),
      new Configuration,
      Some("master,centos-oracle,Maroon:2181")
    )
    // TODO 4 关闭环境
    spark.stop()
  }

}
