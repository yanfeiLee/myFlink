package com.lyf.api.transform

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/15 23:06
  * Version: 1.0
  */
object SplitAndSelect {
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
    //给流打标签，实际并没有真正的分流
    val splitStream = sensorStream.split(
      sensor => {
        if (sensor.temperature > 30) List("high")
        else List("low")
      }
    )
    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")

    val allStream = splitStream.select("high", "low")


    //根据标签，对流进行筛选
    highStream.print("high").setParallelism(1)
    lowStream.print("low").setParallelism(1)
    allStream.print()
    //启动
    env.execute("SplitAndSelect")
  }
}
