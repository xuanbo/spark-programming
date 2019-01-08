package com.example.spark.streaming.hive

import com.example.spark.streaming.KafkaConsumerApp
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.LoggerFactory

/**
  * 消费kafka数据，写入hive
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object KafkaHiveApp {

  private val log = Logger(LoggerFactory.getLogger(KafkaConsumerApp.getClass))

  /**
    * spark conf
    */
  private val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaHiveApp")

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
    val ssc = new StreamingContext(conf, batchDuration)
    val sparkSession = getOrCreateSparkSession()
    // 创建kafka流
    val stream = createKafkaStream(ssc)
    // 消费
    consume(stream, sparkSession)
    // 启动并等待
    startAndWait(ssc)
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
  def consume(stream: InputDStream[ConsumerRecord[String, String]], sparkSession: SparkSession): Unit = {
    stream.foreachRDD { rdd =>
      // 获取offset信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      import sparkSession.implicits.newStringEncoder
      import sparkSession.implicits.rddToDatasetHolder
      // 写入Hive表，指定压缩格式。发现可以自动创建表
      val df = rdd.map(_.value()).toDF("value")
      df.write.mode(SaveMode.Append).format("orc").saveAsTable("test")

      // 异步提交offset
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }

  /**
    * 获取或创建SparkSession
    *
    * @return SparkSession
    */
  def getOrCreateSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .config(conf)
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      .config("hive.metastore.uris", "thrift://crpprdap25:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark
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
