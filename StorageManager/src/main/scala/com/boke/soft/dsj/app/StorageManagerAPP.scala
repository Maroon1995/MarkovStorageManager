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
    //    val sc = CreateSparkContext.getSC("StorageManager")
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
    // 如何实现两个RDD嵌套优化，先使用collect方法将此RDD收集起来，然后进行广播。
    val broadcasterCI: Broadcast[Array[mutable.HashMap[String, Any]]] = sc.broadcast(currentInventorieRDD.collect()) // 广播变量
    // TODO 3-计算结果表：当前库存量、当前库存量能够覆盖未来一个月使用量的概率、历史出库最大值、当前需求提报量
    val ResultsInfoRDD = MaterialQuantityGroups.map {
      case (item_cd, mqiIter) => {
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
        val currentStatusX = sortMqiList.last.status // 当前紧邻状态
        val currentUpper = sortMqiList.last.upper // 当前紧邻状态的最大上限值
        val itemDesc = mqiList.head.item_desc // 物料名称
        var resultsInfo: ResultsInfo = null // 初始化结果
        for (elemMap <- currentInventories) {
          val stock: Option[Any] = elemMap.get(item_cd) //获取当前物料的库存
          resultsInfo = stock match {
            case Some(value) =>
              val double = value.toString.toDouble
              val tuple = ProduceStatus.getStatus(maxQuantity, double) // 当前物料库存值所处的状态和状态上限值
              val stockStatusY = tuple._1
              val stockUpper = tuple._2
              val jSONObjects = PhoenixUtil.queryToJSONObjectList(sql =
                s"""
                   |select "probability" from "STATUS_MATRIX"
                   |where "item_cd" = '${item_cd}'
                   |and "xAxis" = '${currentStatusX}'
                   |and "yAxis" < '${stockStatusY}'
                   |""".stripMargin) // 获取当前紧邻状态和库存状态的覆盖数据
              if (jSONObjects.nonEmpty && jSONObjects != null) {
                val probabilities = jSONObjects.map(_.getDoubleValue("probability"))
                val probability: Double = probabilities.sum // 覆盖着的概率求合，计算当前库存的覆盖率
                ResultsInfo(item_cd, itemDesc, currentYearMonthDays, double, s"${math.round(probability * 100, 2)}%", maxQuantity, 100.0)
              } else { // 若该物料有库存，但是没有历史出库记录。则表示该物料为呆滞物料
                ResultsInfo(item_cd, itemDesc, currentYearMonthDays, double, "9999%", maxQuantity, 0) // 9999%表示仓库呆滞物料
              }
            case None => ResultsInfo(item_cd, itemDesc, currentYearMonthDays, 0.0, "0.00%", maxQuantity, 100.0)
          }
        }
        resultsInfo
      }
    }

    // TODO 4-输出结果
    // (1)输出到mysql
    // RDD\DataSet\DF相互转换的时候，需要导入隐式转换
    import spark.implicits._
    val ResultsInfoDS: Dataset[ResultsInfo] = ResultsInfoRDD.toDS()
    writer.setData(ResultsInfoDS,"material_safety_stock_manager","4",SaveMode.Overwrite)

    // (2)备份到hbase上
    import org.apache.phoenix.spark._
    ResultsInfoRDD.saveToPhoenix(
      tableName = "MATERIAL_SAFETY_STOCK",
      Seq("item_cd", "insert_date", "item_desc", "inventory", "probability", "maxQuantity", "demand"),
      new Configuration,
      Some("master,centos-oracle,Maroon:2181")
    )
    // TODO 5-关闭资源
    sc.stop()
    spark.stop()
  }

}
