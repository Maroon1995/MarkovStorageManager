package com.boke.soft.bean


case class MaterialQuantityInfo(item_cd: String, // 物料编码
                                item_name: String, // 物料名称
                                date_time: String, // 出库日期
                                quantity: Double, // 出库量
                                var status: String = null)
