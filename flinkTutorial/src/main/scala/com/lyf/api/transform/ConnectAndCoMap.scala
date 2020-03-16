package com.lyf.api.transform

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 8:14
  * Version: 1.0
  */
object ConnectAndCoMap {
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
    val high2 = highStream.map(sensor=>(sensor.id,sensor.temperature))

    //connect 连接两个数据结构不一样的流
    val connectStream = high2.connect(lowStream)

    //coMap：对connect后的流，进行各自的映射
    val coMap = connectStream.map(
      high => (high._1, high._2, "warning"),
      low => (low.id, "healthy")
    )
    coMap.print().setParallelism(1)


    //启动
    env.execute("ConnectAndCoMap")
  }
}
