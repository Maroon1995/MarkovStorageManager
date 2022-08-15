package com.boke.soft.dsj.util

import com.boke.soft.dsj.common.Transform
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}

import java.util.Properties

object SparkDBUtil {

  val config: Properties = PropertiesUtil.load("jdbc.properties") // 加载配置文件
  val url = config.getProperty("dbUrl")
  val transform = new Transform()

  /**
   *
   * 初始化配置
   *
   * @param numPartitions :分区数
   * @return
   */
  private def init(numPartitions: String): Properties = {
    val properties = new Properties()
    properties.setProperty("user", config.getProperty("dbUser"))
    properties.setProperty("password", config.getProperty("dbPassword"))
    properties.setProperty("driver", config.getProperty("dbDriver"))
    properties.setProperty("numPartitions", numPartitions)
    properties
  }

  /**
   * 读取Mysql数据
   * @param sqlContext
   */
  class Reader(sqlContext: SQLContext) {

    /**
     * 通过spark sql 的jdbc获取查询表数据
     *
     * @param tableName
     * @param numPartitions
     * @return
     */
    def getQueryData(tableName: String, numPartitions: String): DataFrame = {
      val properties = init(numPartitions)
      val dataFrame: DataFrame = sqlContext.read.jdbc(url, tableName, properties)
      dataFrame
    }

    /**
     *
     * @param tableName
     * @param numPartitions
     * @param predicates : predicates 参数，我们可以通过这个参数设置分区的依据
     * @return
     */
    def getQueryData(tableName: String, numPartitions: String, predicates: Array[String]): DataFrame = {

      val properties = init(numPartitions)
      val dataFrame: DataFrame = sqlContext.read.jdbc(url, tableName, predicates, properties)
      dataFrame
    }
  }


  class Writer {


    /**
     *
     * case "overwrite" => mode(SaveMode.Overwrite)
     * case "append" => mode(SaveMode.Append)
     * case "ignore" => mode(SaveMode.Ignore)
     * case "error" | "errorifexists" | "default" => mode(SaveMode.ErrorIfExists)
     *
     * @param dataSet       : DataSet
     * @param tableName     : 要存如数据的表名称
     * @param numPartitions :分区数
     * @param saveMode      :存储数据方式
     * @tparam T : 存储数据的样例类
     */
    def setData[T](dataSet: Dataset[T], tableName: String, numPartitions: String, saveMode: SaveMode): Unit = {
      val properties = init(numPartitions)
      try {
        dataSet.write.mode(saveMode).jdbc(url, tableName, properties)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

}
