package com.boke.soft.dsj.util

import com.alibaba.fastjson.JSONObject
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

object PhoenixUtil {

  var conn: Connection = _
  var ps: PreparedStatement = _
  var resultSet: ResultSet = _

  private def connection: Connection = {

    // 加载配置文件参数
    val properties = PropertiesUtil.load("jdbc.properties")
    // 驱动类phoenixDriver
    Class.forName(properties.getProperty("phDriver"))
    // 获取连接配置
    conn = DriverManager.getConnection(properties.getProperty("phUrl"))
    conn
  }

  /**
   * 将Hbase中查询的结果返回成ResultSet
   */
  def queryToResultSet(sql: String): ResultSet = {
    // 获取连接配置
    conn = connection
    // 创建数据库操作对象
    ps = conn.prepareStatement(sql)
    // 执行sql语句
    resultSet = ps.executeQuery()
    // 结果输出
    resultSet
  }

  /*
  将Hbase中查询的结果返回成json对象串列表
   */
  def queryToJSONObjectList(sql: String): List[JSONObject] = {
    // 获取连接配置
    val connect = this.connection
    // 创建数据库操作对象
    val pss = connect.prepareStatement(sql)
    // 执行sql语句
    val rs = pss.executeQuery()
    // 处理结果集
    val listBufferJson = new ListBuffer[JSONObject]()
    try {
      val resMetaData = rs.getMetaData // 获取数据的元数据信息
      while (rs.next()) {
        // 创建JSON对象
        val jsonObject = new JSONObject()
        // 将结果封装成Json对象
        //{"erpcode":"xxx","if_new_material":"1"}
        for (i <- 1 to resMetaData.getColumnCount) {
          jsonObject.put(resMetaData.getColumnName(i), rs.getObject(i))
        }
        listBufferJson.append(jsonObject)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 释放资源
      rs.close()
      pss.close()
      connect.close()
    }
    // 结果输出
    listBufferJson.toList
  }

  /**
   * 关闭资源
   */
  def close(): Unit = {
    if (resultSet != null) {
      resultSet.close()
      ps.close()
      conn.close()
    }
  }
}
