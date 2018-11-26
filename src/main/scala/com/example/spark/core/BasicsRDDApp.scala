package com.example.spark.core

import com.typesafe.scalalogging.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * rdd基础概念
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object BasicsRDDApp {

  private[this] val log = Logger(LoggerFactory.getLogger(BasicsRDDApp.getClass))

  private[this] val input = "file:///home/cmccapp/data.txt"
  private[this] val output = "file:///home/cmccapp/wordCount"

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName("BasicsRDDApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 统计文件中单词数
    val totalLength = sc.textFile(input)
      .flatMap(_.split(" "))
      .map(_ => 1)
      .reduce(_ + _)
    log.info("totalLength: {}", totalLength)

    // word count
    sc.textFile(input)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(output)
  }

}
