package com.boke.soft.dsj.bean

case class OriginalData(id: Int,
                        item_cd: String, // 物料编码
                        item_desc: String, // 物料名称
                        var insert_datetime: String, // 出库日期
                        var quantity: Double // 出库量
                       )
