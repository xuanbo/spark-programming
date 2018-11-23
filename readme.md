# 说明

> 基于scala 2.11、spark 2.1.0

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
