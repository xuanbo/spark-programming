package com.example.spark.streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 消费kafka数据，每个分区一个连接、单条写入HBase
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object KafkaHBaseApp {

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
    // 创建kafka流
    val stream = createKafkaStream(ssc)
    // 消费
    consume(stream)
    // 启动并等待
    startAndWait(ssc)
  }

  /**
    * 创建parkContext
    *
    * @return StreamingContext
    */
  def createSparkContext(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaHBaseApp")
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
        // 创建HBase连接
        val conf = createHBaseConf
        val conn = ConnectionFactory.createConnection(conf)
        // 消费，写入HBase
        records.foreach { record =>
          val table = conn.getTable(TableName.valueOf("test_bo"))
          // key作为row id
          val rowId = record.key()
          val put = new Put(Bytes.toBytes(rowId))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(record.value()))
          table.put(put)
          // 关闭
          table.close()
        }
        // 关闭
        conn.close()
      }
      // 异步提交offset
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }

  /**
    * 创建HBase配置
    *
    * @return Configuration
    */
  def createHBaseConf: Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "crpprdap25,crpprdap26,crpprdap27")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf
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
