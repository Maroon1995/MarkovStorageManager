package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{MaterialQuantityInfo, ResultsInfo}
import com.boke.soft.dsj.common.{MyMath, ProduceStatus}
import com.boke.soft.dsj.io.{HBaseReader, MysqlReader}
import com.boke.soft.dsj.process.CreateSparkContext
import com.boke.soft.dsj.produce.Produce
import com.boke.soft.dsj.util.{DateUtil, JDBCUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object StorageManagerAPP {
  def main(args: Array[String]): Unit = {
    // TODO 1-创建环境和实例化对象
    val sc = CreateSparkContext.getSC("StorageManager")
    val hbaseReader = new HBaseReader(sc)
    val mysqlReader = new MysqlReader
    val produce = new Produce(sc)
    // TODO 2-根据物料编码对物料的出库和状态进行分组
    val MaterialQuantityStatus: RDD[MaterialQuantityInfo] = produce.materialQuantityStatusRDD // 获取物料的出库量和状态数据
    val MaterialQuantityStatusMap: RDD[(String, MaterialQuantityInfo)] = MaterialQuantityStatus.map(mqi => (mqi.item_cd, mqi))
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityStatusMap.groupByKey()
    // TODO 3-计算结果表：当前库存量、当前库存量能够覆盖未来一个月使用量的概率、历史出库最大值、当前需求提报量
    val ResultsInfoRDD: RDD[ResultsInfo] = MaterialQuantityGroups.map {
      case (item_cd, mqiIter) =>
        val currentYearMonthDays = DateUtil.getNowTime("yyyy-MM-dd")
        // (1) 统计获取每种物料的当前库存量
        val currentInventories: List[mutable.HashMap[String, Double]] = mysqlReader.currentMaterialStock()
        // (2) 统计计算物料的历史出库最大值
        val mqiList = mqiIter.toList
        val math = new MyMath()
        val maxQuantity = math.getMaxFromList[Double](mqiList.map(_.quantity)) // 计算历史出库最大值
        // (3) 计算当前库存量能够覆盖未来一个月使用量的概率
        val sortMqiList: List[MaterialQuantityInfo] = mqiList.sortWith( //升序排序
          (x, y) => {
            x.insert_datetime < y.insert_datetime
          }
        )
        val currentStatusX = sortMqiList.last.status // 当前紧邻状态
        val currentUpper = sortMqiList.last.upper //当前紧邻状态的最大上限值
        val itemDesc = mqiList.head.item_desc // 物料名称
        var resultsInfo: ResultsInfo = null
        for (elemMap <- currentInventories) {
          val stock: Option[Double] = elemMap.get(item_cd) //获取当前物料的库存
          resultsInfo = stock match {
            case Some(value) => { // ****如果存在库存值
              val tuple = ProduceStatus.getStatus(maxQuantity, value) // 当前物料库存值所处的状态和状态上限值
              val stockStatusY = tuple._1
              val stockUpper = tuple._2
              val jSONObjects = hbaseReader.toJSONObjectList(sql =
                s"""
                   |select "probability" from "STATUS_MATRIX"
                   |where "item_cd" = '${item_cd}'
                   |and "xAxis" = '${currentStatusX}'
                   |and "yAxis" < '${stockStatusY}'
                   |""".stripMargin) // 获取当前紧邻状态和库存状态的覆盖数据
              if (jSONObjects.nonEmpty && jSONObjects != null) {
                val probabilities = jSONObjects.map(_.getDoubleValue("probability"))
                val probability: Double = probabilities.sum // 覆盖着的概率求合，计算当前库存的覆盖率
                ResultsInfo(item_cd, itemDesc, currentYearMonthDays, value, s"${math.round(probability * 100, 2)}%", maxQuantity, 100.0)
              } else { // 若该物料有库存，但是没有历史出库记录。则表示该物料为呆滞物料
                ResultsInfo(item_cd, itemDesc, currentYearMonthDays, value, "9999%", maxQuantity, 0) // 9999%表示仓库呆滞物料
              }
            }
            case None => { // ****如果不存在库存值
              ResultsInfo(item_cd, itemDesc, currentYearMonthDays, 0.0, "0.00%", maxQuantity, 100.0)
            }
          }
        }
        resultsInfo
    }

    // TODO 4-结果输出到mysql
    ResultsInfoRDD.foreachPartition(
      resultsIter => {
        val connection = JDBCUtil.getConnection
        val sql =
          s"""
             |insert into
             |""".stripMargin
        val params: Iterator[Array[Any]] = resultsIter.map(
          ri => Array(
            ri.item_cd, ri.item_desc, ri.insert_date, ri.inventory,
            ri.probability, ri.maxQuantity, ri.demand
          )
        )
        JDBCUtil.setUpdateBatch(connection, sql, params) // 将结果写入到数据库
      }
    )
    // TODO 5-关闭资源
    sc.stop()
  }

}
