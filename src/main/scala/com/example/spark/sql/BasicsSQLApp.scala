package com.example.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 基础sql
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object BasicsSQLApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicsSQLApp")
      .getOrCreate()

    val df = spark.read.json("examples/src/main/resources/people.json")
    df.show()
  }

}
