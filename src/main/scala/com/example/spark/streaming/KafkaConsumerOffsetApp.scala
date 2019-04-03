package com.example.spark.streaming

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 消费kafka
  */
object KafkaConsumerOffsetApp {

  private val log = Logger(LoggerFactory.getLogger(KafkaConsumerOffsetApp.getClass))

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
    "auto.offset.reset" -> "none",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * topic、partition
    */
  private val topicPartitions = Map[String, Int]("bobo" -> 2)


  def main(args: Array[String]): Unit = {
    // 创建redis
    InternalRedisClient.makePool(redisHost = "172.16.213.79")
    // 创建SparkContext
    val ssc = createSparkContext()
    // 创建kafka流
    val stream = createKafkaStream(ssc)
    // 消费
    consume(stream)
    // 启动并等待
    startAndWait(ssc)
  }

  /**
    * 创建sparkContext
    *
    * @return StreamingContext
    */
  def createSparkContext(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaConsumerOffsetApp")
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
    val offsets = getOffsets

    // 创建kafka stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](offsets.keys.toList, kafkaParams, offsets)
    )
    stream
  }

  /**
    * 从redis中获取offset信息
    *
    * @return Map[TopicPartition, Long]
    */
  def getOffsets: Map[TopicPartition, Long] = {
    val jedis = InternalRedisClient.getResource

    // 设置每个分区起始的offset
    val offsets = mutable.Map[TopicPartition, Long]()

    topicPartitions.foreach { it =>
      val topic = it._1
      val partitions = it._2
      // 遍历分区，设置每个topic下对应partition的offset
      for (partition <- 0 until partitions) {
        val topicPartitionKey = topic + ":" + partition
        var lastOffset = 0L
        val lastSavedOffset = jedis.get(topicPartitionKey)

        if (null != lastSavedOffset) {
          try {
            lastOffset = lastSavedOffset.toLong
          } catch {
            case e: Exception =>
              log.error("get lastSavedOffset error", e)
              System.exit(1)
          }
        }
        log.info("from redis topic: {}, partition: {}, lastOffset: {}", topic, partition, lastOffset)

        // 添加
        offsets += (new TopicPartition(topic, partition) -> lastOffset)
      }
    }

    InternalRedisClient.returnResource(jedis)

    offsets.toMap
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

      // 计算相关指标，这里就统计下条数了
      val total = rdd.count()

      val jedis = InternalRedisClient.getResource
      val pipeline = jedis.pipelined()
      // 会阻塞redis
      pipeline.multi()

      // 更新相关指标
      pipeline.incrBy("totalRecords", total)

      // 更新offset
      offsetRanges.foreach { offsetRange =>
        log.info("save offsets, topic: {}, partition: {}, offset: {}", offsetRange.topic, offsetRange.partition, offsetRange.untilOffset)
        val topicPartitionKey = offsetRange.topic + ":" + offsetRange.partition
        pipeline.set(topicPartitionKey, offsetRange.untilOffset + "")
      }

      // 执行，释放
      pipeline.exec()
      pipeline.sync()
      pipeline.close()
      InternalRedisClient.returnResource(jedis)
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
