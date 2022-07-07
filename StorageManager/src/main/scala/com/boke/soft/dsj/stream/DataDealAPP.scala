package com.boke.soft.dsj.stream

import com.alibaba.fastjson.JSON
import com.boke.soft.dsj.bean.OriginalData
import com.boke.soft.dsj.process.CreateStreamingContext.GetSSC
import com.boke.soft.dsj.process.DefAccumulator
import com.boke.soft.dsj.stream.KafkaStream.GetKafkaDStream
import com.boke.soft.dsj.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.Seconds
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
    // 处理获取到数据,并重新封装成OriginalData对象
    val kafkaDStreamOriginalData: DStream[OriginalData] = kafkaDStreamOffset.map(
      recodes => {
        JSON.parseObject(recodes.value(), classOf[OriginalData])
      }
    )
    // 修改日期格式，"2018/9/27 9:07:51" -> "2018/09"
    val kafkaDStreamDealDate = kafkaDStreamOriginalData.mapPartitions {
      odIter => {
        val odList: List[OriginalData] = odIter.toList
        val originalDatas = odList.map(
          od => {
            println(od.insert_datetime)
            od.insert_datetime = DateUtil.formatDateToMonth(od.insert_datetime) // 转换日期格式为年月yyyy/mm
            ((od.item_cd, od.item_desc, od.insert_datetime), od.quantity)
          }
        )
        originalDatas.toIterator
      }
    }.cache()
    //    // 累加器
    //    val accumulator = new DefAccumulator
    //    ssc.sparkContext.register(accumulator, "my_accumulator") //注册累加器

    val kafkaDStreamReduce: DStream[((String, String, String), Double)] = kafkaDStreamDealDate.reduceByKeyAndWindow(
      (x: Double, y: Double) => x + y,
      Seconds(16),
      Seconds(8)
    )

    kafkaDStreamReduce.print()


    //    kafkaDStreamReduce.foreachRDD(
    //      rdd => rdd.foreach(
    //        od => {
    //          accumulator.add(od)
    //        }
    //      )
    //    )

    // 开启
    ssc.start()
    ssc.awaitTermination()
  }
}
