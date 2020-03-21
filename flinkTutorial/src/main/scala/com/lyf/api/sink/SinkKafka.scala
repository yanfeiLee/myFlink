package com.lyf.api.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/20 22:02
  * Version: 1.0
  */
object SinkKafka {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")

    source.addSink(new FlinkKafkaProducer011[String]("hadoop104:9092", "test", new SimpleStringSchema()))

    //启动
    env.execute("SinkKafka")
  }
}
