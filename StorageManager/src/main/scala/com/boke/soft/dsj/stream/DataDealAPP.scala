package com.boke.soft.dsj.stream

import com.alibaba.fastjson.{JSON, JSONObject}
import com.boke.soft.dsj.bean.MaterialQuantityInfo
import com.boke.soft.dsj.process.CreateStreamingContext.GetSSC
import com.boke.soft.dsj.stream.KafkaStream.GetKafkaDStream
import com.boke.soft.dsj.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object DataDealAPP {

  def main(args: Array[String]): Unit = {
    // 创建运行环境和上下文环境对象
    val ssc = GetSSC("datadeal")
    // 主题名称，消费者组名称
    val topicName = "storage_manager_material_quantity"
    val groupId = "storage_manager_topics_mq"
    val kafkaDStreamRecord: InputDStream[ConsumerRecord[String, String]] = GetKafkaDStream(ssc, topicName, groupId)
    // 获取新的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val kafkaDStreamOffset = kafkaDStreamRecord.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }
    // 处理获取到数据
    val kafkaDStreamJSONObject = kafkaDStreamOffset.map(recodes => JSON.parseObject(recodes.value())) // 将json字符串转换成json对象
    kafkaDStreamJSONObject.foreachRDD(
      rdd => rdd.foreach(_.toString)
    )
    // 开启
    ssc.start()
    ssc.awaitTermination()
  }
}
