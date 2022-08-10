package com.boke.soft.dsj.io

import com.alibaba.fastjson.JSON
import com.boke.soft.dsj.common.Transform
import com.boke.soft.dsj.util.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

import java.util.Properties
import scala.collection.mutable
import scala.swing.Table.ElementMode.Column

class MysqlReader(sqlContext: SQLContext) extends Serializable {


  val config: Properties = PropertiesUtil.load("jdbc.properties") // 加载配置文件
  val url = config.getProperty("dbUrl")

  /**
   *
   * 初始化配置
   * @param numPartitions:分区数
   * @return
   */
  private def init(numPartitions:String): Properties ={
    val properties = new Properties()
    properties.setProperty("user", config.getProperty("dbUser"))
    properties.setProperty("password", config.getProperty("dbPassword"))
    properties.setProperty("driver", config.getProperty("dbDriver"))
    properties.setProperty("numPartitions", numPartitions)
    properties
  }

  /**
   * 通过spark sql 的jdbc获取查询表数据
   * @param tableName
   * @param numPartitions
   * @return
   */
  def getQueryData(tableName: String, numPartitions: String): DataFrame = {
    val properties = init(numPartitions)
    val dataFrame:DataFrame = sqlContext.read.jdbc(url, tableName, properties).select(col("item_cd"),col("current_stock"))
    dataFrame
  }

  /**
   *
   * @param tableName
   * @param numPartitions
   * @param predicates: predicates 参数，我们可以通过这个参数设置分区的依据
   * @return
   */
  def getQueryData(tableName: String, numPartitions: String, predicates: Array[String]): DataFrame = {

    val properties = init(numPartitions)
    val dataFrame:DataFrame = sqlContext.read.jdbc(url, tableName, predicates, properties)
    dataFrame
  }


}
