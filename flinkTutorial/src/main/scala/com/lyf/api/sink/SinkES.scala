package com.lyf.api.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/21 8:28
  * Version: 1.0
  */
object SinkES {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")


//    source.addSink()

    //启动
     env.execute("SinkES")
  }
}
