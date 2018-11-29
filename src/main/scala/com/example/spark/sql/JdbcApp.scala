package com.example.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * jdbc
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object JdbcApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JdbcApp")
      .getOrCreate()

    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    val url = "jdbc:mysql://crpprdap25:15026/hdp?useUnicode=true&amp;characterEncoding=UTF-8"
    // 分区的字段列
    val columnName = "id"
    // 分区的下界
    val lowerBound = 1
    // 分区的上界
    val upperBound = 10000
    // 分区数
    val numPartitions = 10
    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", "hdp")
    connectionProperties.put("password", "hdp")


    // Loading data from a JDBC source
    // 该操作将字段colName中1-10000条数据分到10个partition中，使用很方便，缺点也很明显，只能使用整形数据字段作为分区关键字。
    val jdbcDF = spark.read.jdbc(url, "sys_user", columnName, lowerBound, upperBound, numPartitions, connectionProperties)

    // Operators
    jdbcDF.show()

    // Saving data to a JDBC source
    jdbcDF.write.jdbc(url, "sys_user_bak", connectionProperties)
  }

}
