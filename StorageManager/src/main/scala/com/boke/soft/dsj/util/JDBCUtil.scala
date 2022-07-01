package com.boke.soft.dsj.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable.ListBuffer
import com.alibaba.druid.pool.DruidDataSourceFactory


object JDBCUtil {

  // 初始化连接池
  var dataSource: DataSource = init()

  // 初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("jdbc.properties") // 加载配置文件
    properties.setProperty("diverClassName", config.getProperty("dbDriver"))
    properties.setProperty("url", config.getProperty("dbUrl"))
    properties.setProperty("username", config.getProperty("dbUser"))
    properties.setProperty("password", config.getProperty("dbPassword"))
    properties.setProperty("MaxActive", config.getProperty("jdbc.datasource.size"))

    DruidDataSourceFactory.createDataSource(properties) // 德鲁伊连接池
  }

  // 获取mysql连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  // 执行sql:单条数据插入
  def executeUpdateOne(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false) // 不自动提交，保证数据的完整性和干净性（如果中间提交内容错误或不对将不会提交到数据库中）
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject((i + 1), params(i)) // 向sql中设置（添加）参数
        }
      }

      /*
      executeUpdate与execute的区别：
      1、用于执行 INSERT、UPDATE 或 DELETE 语句以及 SQL DDL（数据定义语言）语句，
      例如 CREATE TABLE 和 DROP TABLE。INSERT、UPDATE 或 DELETE 语句的效果是修改表
      中零行或多行中的一列或多列。executeUpdate 的返回值是一个整数（int），指示受影响的行数
      （即更新计数）。对于 CREATE TABLE 或 DROP TABLE 等不操作行的语句，executeUpdate 的返回值总为零。
      2、可用于执行任何SQL语句，返回一个boolean值，表明执行该SQL语句是否返回了ResultSet。
      但它执行SQL语句时比较麻烦，通常我们没有必要使用execute方法来执行SQL语句，而是使用executeQuery或executeUpdate更适合
       */
      rtn = pstmt.executeUpdate()
      connection.commit() // 提交任务
      pstmt.close() // 释放资源
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  // 执行sql:批量插入
  def executeUpdateBatch(connection: Connection, sql: String, paramsList: Iterator[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i)) //设置参数
          }
          pstmt.addBatch() // 将一组参数添加到PreparedStatement对象的批处理命令中
        }
        rtn = pstmt.executeBatch() // 将一批命令提交给数据库来执行，如果全部命令执行成功，则返回更新计数组成的数组。
        connection.commit()
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  // 判断一条数据是否存在
  def isExsist(connection: Connection, sql: String, papams: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      if (papams != null && papams.length > 0) {
        for (i <- papams.indices) {
          pstmt.setObject(i + 1, papams(i))
        }
        flag = pstmt.executeQuery().next()
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  // 从MySql的表中获取一条数据
  def getOneDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result: Long = 0L
    var pstmt: PreparedStatement = null

    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  // 从mySql的表中获取一批数据
  def getBatchDataFromMysql(conn: Connection, sql: String, papams: Array[Any]): List[List[Any]] = {

    val results: ListBuffer[List[Any]] = ListBuffer[List[Any]]()
    var pstmt: PreparedStatement = null
    //
    pstmt = conn.prepareStatement(sql)
    if (papams != null&& papams.length > 0) {
      for (i <- papams.indices) { // 设置参数
        pstmt.setObject(i + 1, papams(i))
      }
    }

    val resultSet = pstmt.executeQuery() // 获取查询结果
    while (resultSet.next()) {
      val row: ListBuffer[Any] = new ListBuffer[Any]()
      for (j <- 1 until (resultSet.getMetaData.getColumnCount + 1)) {
        row.append(resultSet.getObject(j))
      }
      results.append(row.toList)
    }
    results.toList
  }
}
