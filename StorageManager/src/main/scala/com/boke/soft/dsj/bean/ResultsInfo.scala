package com.boke.soft.dsj.bean

case class ResultsInfo(item_cd: String,
                       item_desc: String,
                       insert_date: String,
                       inventory: Double,     // 当前库存量
                       probability: String,  // 当前库存有多大概率覆盖未来一个月的出库量
                       maxQuantity: Double, // 最大出库量
                       demand: Double)      // 当前的需求提报量
