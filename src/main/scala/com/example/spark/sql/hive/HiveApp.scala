package com.example.spark.sql.hive

import org.apache.spark.sql.{Row, SparkSession}

/**
  * 访问hive表
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object HiveApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveApp")
      // 加这个配置访问集群中的hive
      // https://stackoverflow.com/questions/39201409/how-to-query-data-stored-in-hive-table-using-sparksession-of-spark2
      .config("hive.metastore.uris", "thrift://crpprdap25:9083")
      .enableHiveSupport()
      .getOrCreate()

    // 执行sql
    spark.sql("show databases").show()

    // 官方的例子
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    import spark.implicits.newStringEncoder
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
  }

  case class Record(key: Int, value: String)

}
