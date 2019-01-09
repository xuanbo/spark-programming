package com.example.spark.streaming

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * 消费kafka，并开启checkpoint，WAL，ShutdownHook
  * https://www.iteblog.com/archives/1716.html
  */
object KafkaConsumerCheckpointApp {

  private val log = Logger(LoggerFactory.getLogger(KafkaConsumerCheckpointApp.getClass))

  /**
    * 每3s一批数据
    */
  private val batchDuration = Seconds(3)

  /**
    * kakfa参数
    */
  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "crpprdap25:6667,crpprdap26:6667,crpprdap27:6667",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * topic
    */
  private val topics = Array("bobo")

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val ssc = createSparkContext()
    // 开启checkpoint
    ssc.checkpoint("/spark-checkpoint/kafkaConsumerCheckpointApp")
    // 创建kafka流
    val stream = createKafkaStream(ssc)
    // 消费
    consume(stream)
    // 启动并等待
    startAndWait(ssc)
    // 在确认所有receiver都关闭的情况下才终止程序
    sys.addShutdownHook({
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    })
  }

  /**
    * 创建sparkContext
    *
    * @return StreamingContext
    */
  def createSparkContext(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaConsumerCheckpointApp")
    // 设置WAL，将receiver获取数据的存储级别修改为StorageLevel.MEMORY_AND_DISK_SER
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    // 创建StreamingContext
    val ssc = new StreamingContext(conf, batchDuration)
    ssc
  }

  /**
    * 创建kakfa流
    *
    * @param ssc StreamingContext
    * @return InputDStream
    */
  def createKafkaStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }

  /**
    * 消费
    *
    * @param stream InputDStream
    */
  def consume(stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    stream.foreachRDD { rdd =>
      // 获取offset信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 对每个分区进行处理
      rdd.foreachPartition { records =>
        records.foreach { record =>
          log.info("partition: {}, offset: {}, key: {}, value: {}", record.partition, record.offset, record.key, record.value)
        }
      }

      // 异步提交offset
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }

  /**
    * 启动并等待
    *
    * @param ssc StreamingContext
    */
  def startAndWait(ssc: StreamingContext): Unit = {
    // 启动
    ssc.start()
    // 等待
    ssc.awaitTermination()
  }

}
