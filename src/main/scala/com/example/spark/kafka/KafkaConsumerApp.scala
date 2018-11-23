package com.example.spark.kafka

import java.util.{Collections, Properties}

import scala.collection.JavaConversions.iterableAsScalaIterable
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory


/**
  * kafka消费者
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object KafkaConsumerApp {

  private val log = Logger(LoggerFactory.getLogger(KafkaConsumerApp.getClass))

  def main(args: Array[String]): Unit = {
    val brokers = "crpprdap25:6667,crpprdap26:6667,crpprdap27:6667"
    val topic = "bobo"
    // 配置
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    // group
    props.put("group.id", "test")
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", false: java.lang.Boolean)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    // 创建消费者
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singleton(topic))
    // 消费数据
    try {
      while (true) {
        // poll数据
        val records = consumer.poll(500)
        // 处理
        for (record <- records) {
          log.info("partition: {}, offset: {}, key: {}, value: {}", record.partition, record.offset, record.key, record.value)
        }
        // 异步提交offset
        consumer.commitAsync()
      }
    } finally {
      consumer.close()
    }
  }

}
