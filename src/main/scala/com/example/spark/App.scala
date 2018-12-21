package com.example.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * App
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object App {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("App").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5))
      .map(_ * 2)
      .saveAsTextFile("/user/hue/sparkcode/appResult")
    sc.stop()
  }

}
