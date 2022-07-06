package com.boke.soft.dsj.util

import org.apache.kafka.clients.consumer.{ConsumerConfig,  KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable
import scala.util.Try

object OffsetCorrectionUtil {

  // 纠正offset
  def Correction(kafkaParams: Map[String, Object], topics: Seq[String], groupId: String, topicPartitionMap: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val earliestOffsets = getEarliestOffset(kafkaParams, topics, groupId) //获取earliestOffset
    val latestOffset = getLatestOffset(kafkaParams, topics, groupId)
    for ((k, v) <- topicPartitionMap) {
      val current: Long = v
      val earliest: Long = earliestOffsets(k)
      val latest = latestOffset(k)
      if (current < earliest || current > latest) { //如果成立说明需要进行纠偏-->offset到earliest
        topicPartitionMap.put(k, earliest)
      }
    }
    topicPartitionMap
  }

  private def getEarliestOffset(kafkaParams: Map[String, Object], topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    var newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    newKafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    //通过kafka的api去消费
    //这里主要,传入的是java的map,所以需要使用scala的JavaConversions._
    val consumer: KafkaConsumer[String, Object] = new KafkaConsumer[String, Object](newKafkaParams.asJava)
    consumer.subscribe(topics)
    System.out.println(consumer.assignment())
    val pullData = Try {
      consumer.poll(Duration.ofMillis(0))
    }
    if (pullData.isFailure) {
      //邮件报警
    }

    val topicPartitions: Set[TopicPartition] = consumer.assignment().toSet
    //暂停消费
    consumer.pause(topicPartitions)
    //从头开始
    consumer.seekToBeginning(topicPartitions)

    val earliestOffsetMap = topicPartitions.map(line => (line, consumer.position(line))).toMap

    consumer.unsubscribe()
    consumer.close()

    earliestOffsetMap

  }

  private def getLatestOffset(kafkaParams: Map[String, Object], topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    val newKafkaParams = mutable.Map[String, Object]()
    newKafkaParams ++= kafkaParams
    newKafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    newKafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    //通过kafka的api去消费
    val consumer: KafkaConsumer[String, Object] = new KafkaConsumer[String, Object](newKafkaParams.asJava)
    consumer.subscribe(topics)
    val pullData = Try {
      consumer.poll(Duration.ofMillis(0))

    }
    if (pullData.isFailure) {
      //邮件报警
    }
    val topicPartitions: Set[TopicPartition] = consumer.assignment().toSet
    //暂停消费
    consumer.pause(topicPartitions)
    //从尾开始消费
    consumer.seekToEnd(topicPartitions)

    val earliestOffsetMap = topicPartitions.map(line => (line, consumer.position(line))).toMap

    consumer.unsubscribe()
    consumer.close()

    earliestOffsetMap

  }

}
