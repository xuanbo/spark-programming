package com.example.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author 奔波儿灞
  * @since 1.0
  */
object WordCountApp {

  /**
    * hdfs中的输入
    */
  private[this] val input = "/user/hue/sparkcode/wordCountInput/data.txt"

  /**
    * hdfs中的输出
    */
  private[this] val output = "/user/hue/sparkcode/wordCountResult"

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName("WordCountApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // word count
    sc.textFile(input)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(output)
  }

}
