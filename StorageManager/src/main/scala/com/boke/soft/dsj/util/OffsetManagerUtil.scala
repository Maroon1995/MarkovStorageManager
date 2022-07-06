package com.boke.soft.dsj.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.util


/**
 * kafka偏移量管理工具类，用于对每个partition分区的偏移量的读取和保存
 */
object OffsetManagerUtil {

  /**
   *
   * --------保存当前消费者组-消费的主题-分区的偏移量到Redis---------------------
   * hash类型 HMSET key field-value : 同时对个field-value(域-值)进行保存
   * key => offset:topic:groupId
   * field => partition (分区)
   * value => 结束点的偏移量untilOffset
   *
   * @param topicName   主题名称
   * @param groupId     消费者组
   * @param offsetRange 当前消费者组中，消费的主题对应分区的偏移量的起始和结束信息
   */
  def saveOffset(topicName: String, groupId: String, offsetRange: Array[OffsetRange]): Unit = {
    // 定义Java的map集合，用于向Redis中保存数据[field-value]
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    // 遍历offsetRange
    for (offset <- offsetRange) {
      val partition = offset.partition //获取当前消费主题对应的分区
      val untilOffset = offset.untilOffset //获取当前消费后的数据偏移量的结束点
      offsetMap.put(partition.toString, untilOffset.toString) // 封装成HashMap
      println("保存分区：" + partition + " 偏移量范围:" + offset.fromOffset + "-->" + offset.untilOffset)
    }
    if (offsetMap != null && offsetMap.size() > 0) {
      // 保存到redis中 hash类型 HSET offset:topic:groupId partition untilOffset
      val jedisClient = RedisUtil.getJedisClient // 获取redis客户端连接
      val offsetKey = "offset:" + topicName + ":" + groupId // 拼接hset的key
      jedisClient.hmset(offsetKey, offsetMap) // 保存数据到redis中去

      jedisClient.close() //关闭资源
    }
  }

  /**
   * --------向Redis读取当前消费者组-消费的主题-分区的偏移量---------------------
   *
   * @param topicName
   * @param groupId
   * @param offsetRange
   */
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {

    val jedisClient = RedisUtil.getJedisClient // 获取客户端
    val offsetKey = "offset:" + topicName + ":" + groupId //拼接key
    val offsetMap: util.Map[String, String] = jedisClient.hgetAll(offsetKey) // 获取所以主题分区的untilOffset
    jedisClient.close() // 关闭资源
    // 因为kafka消费数据时，进行的离散化流getKafkaStream方法，需要传入的offset的类型为Map[TopicPartition, Long]
    // 因此需要将 util.Map[String, String] => Map[TopicPartition, Long]
    // 为了方便操作 需要将java的map结合转换为scala集合
    import scala.collection.JavaConverters._
    val offset: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, untilOffset) =>
        println("分区:" + partition + " 截至偏移量:" + untilOffset)
        (new TopicPartition(topicName, partition.toInt), untilOffset.toLong) // 将Redis中保存的分区对应的偏移量进行封装
    }.toMap
    offset
  }
}
