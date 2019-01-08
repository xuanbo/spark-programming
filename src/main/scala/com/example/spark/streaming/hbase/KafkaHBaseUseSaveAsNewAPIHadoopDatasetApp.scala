package com.example.spark.streaming.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 消费kafka数据，每个分区一个连接、使用saveAsNewAPIHadoopDataset直接写HBase
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object KafkaHBaseUseSaveAsNewAPIHadoopDatasetApp {

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
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaHBaseUseSaveAsNewAPIHadoopDatasetApp")
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
    val conf = createHBaseConf
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    stream.foreachRDD { rdd =>
      // 获取offset信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.map { record =>
        val rowId = record.key()
        val put = new Put(Bytes.toBytes(rowId))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(record.value()))
        (new ImmutableBytesWritable, put)
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

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
    conf.set(TableOutputFormat.OUTPUT_TABLE, "test_bo")
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
