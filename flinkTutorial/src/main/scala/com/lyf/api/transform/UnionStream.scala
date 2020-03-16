package com.lyf.api.transform

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 8:30
  * Version: 1.0
  */
object UnionStream {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")
    val sensorStream = source.map(line => {
      val fields = line.split(",")
      SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
    }
    )
    val splitStream = sensorStream.split(
      sensor => {
        if (sensor.temperature > 30) {
          List("high")
        } else {
          List("low")
        }
      }
    )
    val lowStream = splitStream.select("low")
    val highStream = splitStream.select("high")
    val high2 = highStream.map(sensor => (sensor.id, sensor.temperature))

    //union 合并两个数据结构一样的流
    val unionStream = lowStream.union(highStream)
    unionStream.print().setParallelism(1)
    //启动
    env.execute("UnionStream")
  }
}
