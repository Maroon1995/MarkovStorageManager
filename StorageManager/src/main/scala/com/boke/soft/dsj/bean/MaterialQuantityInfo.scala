package com.boke.soft.dsj.bean

case class MaterialQuantityInfo(item_cd: String, // 物料编码
                                item_desc: String, // 物料名称
                                insert_datetime: String, // 出库日期
                                quantity: Double, // 出库量
                                var status: String = null, // 状态
                                var upper: Int = 0 //每个状态段的数值上限
                               )
