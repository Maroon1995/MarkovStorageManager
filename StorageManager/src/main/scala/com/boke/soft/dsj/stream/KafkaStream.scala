package com.boke.soft.dsj.stream

import com.boke.soft.dsj.util.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object KafkaStream {

  /**
   * 读取Kafka主题中数据流
   *
   * @param ssc       streaming运行时环境和对象
   * @param topicName : 主题
   * @param groupId   : 消费者组
   * @return 返回数据流
   */
  def GetKafkaDStream(ssc: StreamingContext, topicName: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    val offset = OffsetManagerUtil.getOffset(topicName, groupId) //获取offset
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null && offset.nonEmpty) {
      // 从已有的offset处开始读取数据
      kafkaDStream = MyKafkaUtil.getKafkaStream(ssc, topicName, groupId, offset)
    } else {
      // 从最新的offset处开始读取数据
      kafkaDStream = MyKafkaUtil.getKafkaStream(ssc, topicName, groupId)
    }
    kafkaDStream
  }

  /**
   * 获取新的偏移量
   * @param kafkaDStream: 从kafka中获取到的数据流
   * @return
   */
  def GetNewOffsetRanges(kafkaDStream: InputDStream[ConsumerRecord[String, String]]): Tuple2[DStream[ConsumerRecord[String, String]], Array[OffsetRange]] = {
    var offsetRanges = Array.empty[OffsetRange]
    val offsetKafkaDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }
    (offsetKafkaDStream, offsetRanges)
  }
}
