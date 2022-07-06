package com.boke.soft.dsj.bean

case class StatusMatrix(item_cd: String,
                        xAxis: String,
                        yAxis: String,
                        var probability: Double) // 概率值
