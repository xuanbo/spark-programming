package com.example.spark.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * kafka生产者
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object KafkaProducerApp {

  def main(args: Array[String]): Unit = {
    val brokers = "crpprdap25:6667,crpprdap26:6667,crpprdap27:6667"
    val topic = "bobo"
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // 创建生产者
    val producer = new KafkaProducer[String, String](props)
    // 发送数据
    for (i <- 1 to 1000000) {
      producer.send(new ProducerRecord(topic, String.valueOf(i), String.valueOf(i)))
    }
    // 关闭
    producer.close()
  }

}
