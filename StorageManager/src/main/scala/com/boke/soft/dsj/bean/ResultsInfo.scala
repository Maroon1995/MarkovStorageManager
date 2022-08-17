package com.boke.soft.dsj.bean

case class ResultsInfo(item_cd: String,
                       item_desc: String,
                       insert_date: String,
                       inventory: Double, // 当前库存量
                       maxQuantity: Double, // 最大出库量
                       demand: Double, // 当前的需求提报量
                       currentQuantity: Double,// 当前出库量
                       currentStatusX:String,// 当前出库量所在状态
                       currentUpper:Double, // 当前出库状态的区间上线
                       inventoryStatusY: String, // 当前库存状态
                       inventoryUpper: Double, // 当前库存状态的区间上线
                       var probability: String = null // 当前库存有多大概率覆盖未来一个月的出库量
                      )
