package com.chen.spark


import com.chen.Utils.DateUtil
import com.chen.entity.{EngineCount, KeyWordCount, LogCleaned}
import com.chen.dao.{EngineDao, KeyWordDao}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object LogStreaming {

  def main(args: Array[String]): Unit = {

    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val value = messages.map( x => {
      x.value()
    })

    //156.168.143.87 - - [2018-05-06 14:20:40] "GET /admin/login.php HTTP/1.1" 200 0 "http://www.sogou.com/web?query=hadoop" "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)" "-"
    //original data ==>  cleaned data
    val cleanedData = value.map( line => {
      val infos = line.split(" ")
      val ip = infos(0)
      val time = infos(3).substring(1,infos(3).length)+"-"+infos(4).substring(0,infos(4).length-1)
      val state = infos(8)
      var keyword = "-"
      var engine = "-"
      if(infos(10) != """"-"""") {
        engine = infos(10).substring(infos(10).indexOf(".")+1, infos(10).lastIndexOf("."))
        keyword = infos(10).substring(infos(10).lastIndexOf("=")+1, infos(10).length-1)
      }
      LogCleaned(ip, DateUtil.parseTime(time), state, keyword, engine)
    }).filter( x => x.keyword!="-" && x.state!="0")


    // target 1 : keyword count sorted
    cleanedData.map( record => (record.keyword, 1)).reduceByKey(_+_).foreachRDD(
      rdd => { rdd.foreachPartition(
          partitionRecords => {
            val list = new ListBuffer[KeyWordCount]
            partitionRecords.foreach({
              data => {
                list.append(KeyWordCount(data._1,data._2.toLong))
              }
            })
            KeyWordDao.save(list)
          })
      })

    // target 2: engine count sorted
    cleanedData.map( record => (record.engine, 1)).reduceByKey(_+_).foreachRDD(
      rdd => { rdd.foreachPartition(
        partitionRecords => {
          val list = new ListBuffer[EngineCount]
          partitionRecords.foreach({
            data => {
              list.append(EngineCount(data._1,data._2.toLong))
            }
          })
          EngineDao.save(list)
        })
      })

    value.print()

    ssc.start()

    ssc.awaitTermination()

    ssc.stop()

  }
}
