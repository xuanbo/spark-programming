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
    val connectionProperties = new Properties()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", "hdp")
    connectionProperties.put("password", "hdp")

    // Loading data from a JDBC source
    val jdbcDF = spark.read.jdbc(url, "sys_user", connectionProperties)

    // Operators
    jdbcDF.show()

    // Saving data to a JDBC source
    jdbcDF.write.jdbc(url, "sys_user_bak", connectionProperties)
  }

}
