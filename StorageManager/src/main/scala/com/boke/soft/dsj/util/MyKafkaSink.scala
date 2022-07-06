package com.boke.soft.dsj.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties

object MyKafkaSink {
  // 读取kafka的broker配置信息
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null
  // 添加配置信息
  def creatKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, (true: java.lang.Boolean))
    // 创建kafkProducer
    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  /*
  当key为读取数据的偏移量时
   */
  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = creatKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  /*
  当key被指定时
   */
  def send(topic: String, key: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = creatKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }
}
