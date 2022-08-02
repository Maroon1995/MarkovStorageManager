package com.boke.soft.dsj.bean

case class ResultsInfo(item_cd: String,
                       item_desc: String,
                       var inventory: Double=null,     // 当前库存量
                       var probability: String = null,  // 当前库存有多大概率覆盖未来一个月的出库量
                       var maxQuantity: Double = null, // 最大出库量
                       var demand: Double = null)      // 当前的需求提报量
