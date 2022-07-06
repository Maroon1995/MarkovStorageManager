package com.boke.soft.dsj.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.lang
import java.util.Properties


object MyKafkaUtil {

  private val proper: Properties = PropertiesUtil.load("config.properties")
  val kafkaBrokerList: String = proper.getProperty("kafka.broker.list")

  // kafka消费者配置——定义可变集合
  private val kafkaParams= collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokerList,
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall2021_group", // 用于标识这个消费者属于哪个消费者团体
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer], // 指定key和value的序列化方式
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest", // 自动重置偏移量为最新的偏移量
    // 如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
    // 如果是false，会需要手动维护kafka偏移量
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
  )

  /**
   * 使用基础默认配置创建DStream,返回接收到的输入数据
   *
   * @param ssc
   * @param topic
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )

    dStream
  }

  /**
   * 指定自己的消费团体来创建DStream,返回接收到的数据
   *
   * @param ssc
   * @param topic
   * @param groupID
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topic: String, groupID: String): InputDStream[ConsumerRecord[String, String]] = {

    kafkaParams(ConsumerConfig.GROUP_ID_CONFIG) = groupID

    val dStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // 位置策略：采用距离策略（PreferConsistent）来查找是否为同一个进程，同一个节点，同一个机架
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams) // 消费策略,采用的是订阅策略（Subscribe）
    )
    dStream
  }

  /**
   * 指定自定义的偏移量：
   *
   * @param ssc
   * @param topic
   * @param groupID
   * @param offset ： Map[TopicPartition, Long]) 某个topic分区的偏移量位置（Long）
   * @return
   */
  def getKafkaStream(ssc: StreamingContext, topic: String, groupID: String, offset: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {

    kafkaParams(ConsumerConfig.GROUP_ID_CONFIG) = groupID

    val dStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // 位置策略：采用距离策略（PreferConsistent）来查找是否为同一个进程，同一个节点，同一个机架
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams, offset) // 消费策略,采用的是订阅策略（Subscribe）
    )
    dStream
  }
}