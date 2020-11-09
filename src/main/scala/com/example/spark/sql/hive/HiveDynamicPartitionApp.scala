package com.example.spark.sql.hive

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author 奔波儿灞
  * @since 1.0
  */
object HiveDynamicPartitionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HiveApp")
      .config("hive.metastore.uris", "thrift://hdp02:9083")
      .config("spark.sql.warehouse.dir", "/data/warehouse/tablespace/managed/hive")
      .config("metastore.catalog.default", "hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    import spark.implicits.newProductEncoder
    import spark.implicits.localSeqToDatasetHolder

    val df = Seq(
      (1, "First Value", "2010-01-01"),
      (2, "Second Value", "2010-02-01")
    ).toDF("num", "name", "date_d")

    df.show()

    df.write.partitionBy("date_d").mode(SaveMode.Overwrite).saveAsTable("test")
  }

}
