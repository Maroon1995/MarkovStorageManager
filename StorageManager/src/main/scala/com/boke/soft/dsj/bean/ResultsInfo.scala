package com.boke.soft.dsj.bean

case class ResultsInfo(item_cd: String,
                       item_desc: String,
                       var inventory: Double = null,
                       var probability: String = null,
                       var maxQuantity: Double = null,
                       var demand: Double = null)
