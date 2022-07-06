package com.boke.soft.dsj.io

import com.alibaba.fastjson.{JSON, JSONObject}
import com.boke.soft.dsj.common.MonitorStopSpark
import com.boke.soft.dsj.process.CreateStreamingContext.GetSSC
import com.boke.soft.dsj.stream.KafkaStream.{GetKafkaDStream, GetNewOffsetRanges}
import com.boke.soft.dsj.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.commons.lang.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import java.util.Date

object ReadDBMaxwellSplitFlowAPP {

  def main(args: Array[String]): Unit = {
    // TODO 1 创建环境
    val ssc = GetSSC("MaxwellSplitFlow")
    // TODO 2 从kafka中读取数据流
    val topic = "storage_material_quantity"
    val groupID = "storage_manager_topics"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = GetKafkaDStream(ssc, topic, groupID)
    // TODO 3 获取当前批次读取kafka主题中偏移量信息
//    val tuple: (DStream[ConsumerRecord[String, String]], Array[OffsetRange]) = GetNewOffsetRanges(kafkaDStream)
//    val offsetKafkaDStream = tuple._1
//    val offsetRanges:Array[OffsetRange] = tuple._2
    var offsetRanges = Array.empty[OffsetRange]
    val offsetKafkaDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }
    // TODO 4 对读取的数据进行结构转换
    val jsonObKafakDStream = offsetKafkaDStream.map {
      record =>
        val recordValue = record.value()
        // 将json字符串转换成json对象
        val jsonObject: JSONObject = JSON.parseObject(recordValue)
        jsonObject
    }
    // TODO 5 分流
    jsonObKafakDStream.foreachRDD {
      rdd =>
        rdd.foreach {
          jsonObject =>
            val typeSql: String = jsonObject.getString("type")
            // 获取操作的数据
            val dataJsonObj = jsonObject.getJSONObject("data")
            if (dataJsonObj != null && dataJsonObj.size > 0 && typeSql.equals("insert")) {
              // -----------------------------------------------------------
              // 获取更新的表名称
              val tableName: String = jsonObject.getString("table")
              // 拼接发送的主题
              val sendTopic = "storage_manager_" + tableName
              MyKafkaSink.send(sendTopic, dataJsonObj.toString)
            }
        }
        // 提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupID, offsetRanges)
        val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        System.out.println(s"-------------${dateFormat.format(new Date())}--------------")
    }
    // 开启一个新的线程作为执行停止流服务的程序：优雅的关闭
    new Thread(new MonitorStopSpark(ssc)).start()
    // TODO 6 启动流
    ssc.start()
    ssc.awaitTermination()
  }
}
