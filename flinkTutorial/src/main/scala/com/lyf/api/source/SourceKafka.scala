package com.lyf.api.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/15 21:48
  * Version: 1.0
  */
object SourceKafka {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //构造参数
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop104:9092")
    props.setProperty("group.id", "flink-api")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")
    val topic = "sensor"

    val res = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), props))

    res.print("kafka").setParallelism(1)

    //启动
    env.execute("SourceKafka")

  }
}
