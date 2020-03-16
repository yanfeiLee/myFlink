package com.lyf.api.transform

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/15 22:15
  * Version: 1.0
  */
object reduceTransform {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")


    val sensorStream = source
      .map(
        line => {
          val fields = line.split(",")
          SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
        }
      )

    val keyedStream = sensorStream.keyBy(0)

    val res = keyedStream.reduce(
      (currSensor, newSensor) => SensorReading(currSensor.id, newSensor.timestamp, currSensor.temperature.max(newSensor.temperature))
    )
    res.print().setParallelism(1)

    //启动
    env.execute("reduceTransform")
  }
}
