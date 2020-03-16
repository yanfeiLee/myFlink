package com.lyf.api.source

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.scala._


/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/14 10:30
  * Version: 1.0
  */
object SourceSet {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val res = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    res.print("source Set").setParallelism(1)



    //启动
    env.execute("SourceSet")
  }
}
