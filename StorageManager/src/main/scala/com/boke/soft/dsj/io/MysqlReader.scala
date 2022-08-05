package com.boke.soft.dsj.io

import com.boke.soft.dsj.util.{DateUtil, JDBCUtil}

import scala.collection.mutable

class MysqlReader extends Serializable {

  val connection = JDBCUtil.getConnection

  def currentMaterialStock(): List[mutable.HashMap[String, Double]] ={
    val currentYearMonth = DateUtil.getNowTime("yyyy-MM-dd")
    val currentInventories: List[mutable.HashMap[String, Double]] = JDBCUtil.getBatchDataToHashMap(
      connection,
      sql = "select item_cd, current_stock from material_stock where insert_datetime = ?",
      Array(currentYearMonth)
    )
    currentInventories
  }

}
