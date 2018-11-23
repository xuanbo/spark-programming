package com.example.spark

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
  * App
  *
  * @author 奔波儿灞
  * @since 1.0
  */
object App {

  private val log = Logger(LoggerFactory.getLogger(App.getClass))

  def main(args: Array[String]): Unit = {
    log.info("Hello World")
  }

}
