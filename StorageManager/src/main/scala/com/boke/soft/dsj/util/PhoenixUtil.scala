package com.boke.soft.dsj.util

import com.alibaba.fastjson.JSONObject
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

object PhoenixUtil {

  /*
  将Hbase中查询的结果返回成json对象串列表
   */
  def queryList(sql: String): List[JSONObject] = {
    // 加载配置文件参数
    val properties = PropertiesUtil.load("jdbc.properties")
    // 驱动类phoenixDriver
    Class.forName(properties.getProperty("phDriver"))
    // 获取连接配置
    val conn: Connection = DriverManager.getConnection(properties.getProperty("phUrl"))
    // 创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    // 执行sql语句
    val resultSet: ResultSet = ps.executeQuery()
    val resMetaData = resultSet.getMetaData // 获取数据的元数据信息
    // 处理结果集
    val listBufferJson = new ListBuffer[JSONObject]()
    while (resultSet.next()) {
      // 创建JSON对象
      val jsonObject = new JSONObject()
      // 将结果封装成Json对象
      //{"erpcode":"xxx","if_new_material":"1"}
      for (i <- 1 to resMetaData.getColumnCount) {
        jsonObject.put(resMetaData.getColumnName(i), resultSet.getObject(i))
      }
      listBufferJson.append(jsonObject)
    }
    // 释放资源
    resultSet.close()
    ps.close()
    conn.close()
    // 结果输出
    listBufferJson.toList
  }
}
