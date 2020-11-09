# 说明

> 基于scala 2.11、spark 2.3.2

## spark-streaming

主要是消费kafka数据，直接写入HBase

### 单条

每个partition一个连接，单条数据写入到HBase: `com.example.spark.streaming.KafkaHBaseApp`

```scala
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
```

备注: **性能最低**

### 批量

每个partition一个连接，数据批量写入到HBase: `com.example.spark.streaming.KafkaHBaseBulkApp`:

```scala
def consume(stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
  stream.foreachRDD { rdd =>
    // 获取offset信息
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // 转化成put，然后批量写入
    rdd.map { record =>
      val rowId = record.key()
      val put = new Put(Bytes.toBytes(rowId))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("value"), Bytes.toBytes(record.value()))
      put
    }.foreachPartition { puts =>
      // 创建HBase连接
      val conn = ConnectionFactory.createConnection(conf)
      val table = conn.getTable(TableName.valueOf("test_bo"))
      // 批量写入
      import scala.collection.JavaConversions.seqAsJavaList
      table.put(seqAsJavaList(puts.toList))
      // 关闭
      table.close()
      conn.close()
    }
    // 异步提交offset
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }
}
```

备注: **性能快了一大截**，但要防止一批数据量太大，建议是每一批次的间隔小一点

### saveAsNewAPIHadoopDataset

每个partition一个连接，数据通过saveAsNewAPIHadoopDataset写入到HBase: `com.example.spark.streaming.KafkaHBaseUseSaveAsNewAPIHadoopDatasetApp`:

```scala
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
```

备注: **性能快了一大截，略快于批量**，实际中还需更精确的测试。

### kafka偏移量保存到redis

这里使用jedis，作为redis的客户端。

```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>2.9.0</version>
</dependency>
```

定义一个连接池

```scala
package com.example.spark.streaming

import java.util.Objects

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * redis client
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object InternalRedisClient extends Serializable {

  @transient
  private var pool: JedisPool = _

  def makePool(redisHost: String = "localhost",
               redisPort: Int = 6379,
               redisTimeout: Int = 3000,
               maxTotal: Int = 16,
               maxIdle: Int = 8,
               minIdle: Int = 2,
               testOnBorrow: Boolean = true,
               testOnReturn: Boolean = false,
               maxWaitMillis: Long = 10000): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

      sys.addShutdownHook {
        pool.destroy()
      }
    }
  }

  private def getPool: JedisPool = {
    Objects.requireNonNull(pool)
  }

  def getResource: Jedis = {
    getPool.getResource
  }

  def returnResource(jedis: Jedis): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }
}

```

kafka stream从redis中获取偏移量，并将offset提交到redis

```scala
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
```

主要是：

* `getOffsets`方法从redis中获取到topic对应的partition中的offset
* `consume`消费完数据后，将offset提交到redis

参考：[实时流计算、Spark Streaming、Kafka、Redis、Exactly-once、实时去重](http://lxw1234.com/archives/2018/02/901.htm)

核心是利用redis的`pipeline`、`multi`来保证spark streaming的计算结果写入redis是原子性。

## spark-sql

### 连接hive

经过测试，HDP集群中，需要添加`hive.metastore.uris`、`spark.sql.warehouse.dir`、`metastore.catalog.default`即可访问hive集群

```scala
val spark = SparkSession
  .builder()
  .appName("HiveApp")
  // 加这个配置访问集群中的hive
  .config("hive.metastore.uris", "thrift://crpprdap25:9083")
  .config("spark.sql.warehouse.dir", "/data/warehouse/tablespace/managed/hive")
  .config("metastore.catalog.default", "hive")
  .enableHiveSupport()
  .getOrCreate()

// 执行sql
spark.sql("show databases").show()
```

注意需要添加`spark-hive`依赖：

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_2.11</artifactId>
    <version>${spark.version}</version>
</dependency>
```