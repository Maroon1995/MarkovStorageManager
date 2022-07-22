package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{MaterialQuantityInfo, ResultsInfo}
import com.boke.soft.dsj.common.MyMath
import com.boke.soft.dsj.process.CreateSparkContext
import com.boke.soft.dsj.produce.Produce
import com.boke.soft.dsj.util.JDBCUtil
import org.apache.spark.rdd.RDD

object StorageManagerAPP {
  def main(args: Array[String]): Unit = {
    val sc = CreateSparkContext.getSC
    val connection = JDBCUtil.getConnection
    // 统计获取每种物料的当前库存量
    val currentInventories: List[List[Any]] = JDBCUtil.getBatchDataFromMysql(
      connection,
      sql = "",
      Array()
    )
    // 统计获取每种物料的历史出库最大值
    // 聚合与分组
    val produce = new Produce(sc)
    val MaterialQuantityStatus: RDD[MaterialQuantityInfo] = produce.materialQuantityStatusRDD // 获取物料的出库量和状态数据
    val MaterialQuantityStatusMap: RDD[(String, MaterialQuantityInfo)] = MaterialQuantityStatus.map(mqi => (mqi.item_cd, mqi))
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityStatusMap.groupByKey()
    val ResultsInfoRDD: RDD[ResultsInfo] = MaterialQuantityGroups.map {
      case (item_cd, mqiIter) =>
        val mqiList = mqiIter.toList
        val math = new MyMath()
        val maxQuantity = math.getMaxFromList[Double](mqiList.map(_.quantity)) // 计算历史出库最大值
        val itemDesc = mqiList.head.item_desc // 物料名称

        ResultsInfo(item_cd, itemDesc, maxQuantity = maxQuantity)

    }


    // 统计获取每种物料的当前需求提报量

    // 计算当前库存量能够覆盖未来一个月的出库量的概率

    // 结果输出

    // 关闭资源
    sc.stop()
  }

}
