package com.boke.soft.dsj.app

import com.boke.soft.dsj.bean.{MaterialQuantityInfo, ResultsInfo}
import com.boke.soft.dsj.common.{MyMath, ProduceStatus, Transform}
import com.boke.soft.dsj.io.MysqlReader
import com.boke.soft.dsj.process.CreateSpark
import com.boke.soft.dsj.produce.Produce
import com.boke.soft.dsj.util.{DateUtil, PhoenixUtil, SparkDBUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

import scala.collection.mutable

object StorageManagerAPP {
  def main(args: Array[String]): Unit = {
    // TODO 1-创建环境和实例化对象
    val spark: SparkSession = CreateSpark.getSpark("StorageManager")
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val mysqlReader = new MysqlReader(sqlContext)
    val transform = new Transform()
    val produce = new Produce(spark)
    val writer = new SparkDBUtil.Writer
    val math = new MyMath()

    // TODO 2-根据物料编码对物料的出库和状态进行分组
    val MaterialQuantityStatus: RDD[MaterialQuantityInfo] = produce.materialQuantityStatusRDD // 获取物料的出库量和状态数据
    val MaterialQuantityStatusMap: RDD[(String, MaterialQuantityInfo)] = MaterialQuantityStatus.map(mqi => (mqi.item_cd, mqi))
    val MaterialQuantityGroups: RDD[(String, Iterable[MaterialQuantityInfo])] = MaterialQuantityStatusMap.groupByKey()
    val currentInventorieDF = mysqlReader.getQueryData(tableName = "material_stock", "4").select(col("item_cd"), col("current_stock"))
    val currentInventorieRDD = transform.dataFrameToHashMapRDD(currentInventorieDF)
    val inventoriesRDD: RDD[(String, Double)] = currentInventorieRDD.map {
      hashMap => {
        val key = Some(hashMap.get("item_cd")).value.get
        val values = Some(hashMap.get("current_stock")).value.get
        (key.toString, values.toString.toDouble)
      }
    }
    // 如何实现两个RDD嵌套优化，先使用collect方法将此RDD收集起来，然后进行广播。
    val broadcasterCI = sc.broadcast(inventoriesRDD.collect()) // 广播变量
    // TODO 3-统计计算：当前库存量、当前库存量能够覆盖未来一个月使用量的概率、历史出库最大值、当前需求提报量等
    val ResultsInfoOtherRDD = MaterialQuantityGroups.map {
      case (item_cd, mqiIter) =>
        val currentYearMonthDays = DateUtil.getNowTime("yyyy-MM-dd")
        // (1) 统计获取每种物料的当前库存量
        // 在嵌套RDD中调用此广播变量
        val currentInventories = broadcasterCI.value
        // (2) 统计计算物料的历史出库最大值
        val mqiList = mqiIter.toList
        val maxQuantity = math.getMaxFromList[Double](mqiList.map(_.quantity)) // 计算历史出库最大值
        // (3) 计算当前库存量能够覆盖未来一个月使用量的概率
        val sortMqiList: List[MaterialQuantityInfo] = mqiList.sortWith( //升序排序
          (x, y) => {
            x.insert_datetime < y.insert_datetime
          }
        )
        val currentQuantity = sortMqiList.last.quantity //当前出库量
        val currentStatusX = sortMqiList.last.status // 当前紧邻状态
        val currentUpper = sortMqiList.last.upper // 当前紧邻状态的最大上限值
        val itemDesc = mqiList.head.item_desc // 物料名称
        val inventoriesMap: Map[String, Double] = currentInventories.toMap
        val stock = inventoriesMap.get(item_cd) // 获取物料的当前库存量
        val tuple = stock match {
          case Some(value) => // 有库存值
            val tuple = ProduceStatus.getStatus(maxQuantity, value) // 当前物料库存值所处的状态和状态上限值
            val stockStatusY = tuple._1
            val stockUpper = tuple._2
            ResultsInfo(item_cd, itemDesc, currentYearMonthDays, value, maxQuantity, 100.0,
              currentQuantity, currentStatusX, currentUpper, stockStatusY, stockUpper)
          case None => // 没有库存值
            ResultsInfo(item_cd, itemDesc, currentYearMonthDays, 0.0, maxQuantity, 100.0,
              currentQuantity, currentStatusX, currentUpper, null, 0.0, "0.00%")
        }
        tuple
    }

    val ResultsInfoRDD: RDD[ResultsInfo] = ResultsInfoOtherRDD.map(
      resultsInfo => {
        val sql =
          s"""
             |select "probability" from "STATUS_MATRIX"
             |where "item_cd" = '${resultsInfo.item_cd}'
             |and "xAxis" = '${resultsInfo.currentStatusX}'
             |and "yAxis" <= '${resultsInfo.inventoryStatusY}'
             |""".stripMargin
        val jSONObjects = PhoenixUtil.queryToJSONObjectList(sql) // 获取当前紧邻状态和库存状态的覆盖数据
        val probabilities = jSONObjects.map(_.getDoubleValue("probability"))
        if (probabilities.nonEmpty && probabilities != null) {
          val probability = probabilities.sum
          resultsInfo.probability = s"${math.round(probability * 100, 2)}%"
        } else { // 该物料有库存，但是没有历史出库记录。表示该物料为呆滞物料
          resultsInfo.probability = "9999%"
        }
        resultsInfo
      }
    )

    ResultsInfoRDD.foreach(println)
    // TODO 4-输出结果
    // (1)输出到mysql
    // RDD\DataSet\DF相互转换的时候，需要导入隐式转换
    import spark.implicits._
    val ResultsInfoDS: Dataset[ResultsInfo] = ResultsInfoRDD.toDS()
    writer.setData(ResultsInfoDS, "material_safety_stock_manager", "4", SaveMode.Overwrite)

    // (2)备份到hbase上
    import org.apache.phoenix.spark._
    ResultsInfoRDD.saveToPhoenix(
      tableName = "MATERIAL_SAFETY_STOCK",
      Seq("item_cd", "item_desc", "insert_date", "inventory", "maxQuantity", "demand",
        "currentQuantity","currentStatusX","currentUpper","inventoryStatusY","inventoryUpper", "probability"),
      new Configuration,
      Some("master,centos-oracle,Maroon:2181")
    )
    // TODO 5-关闭资源
    sc.stop()
    spark.stop()
  }

}