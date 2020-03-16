package com.lyf.api.udf

import com.lyf.beans.SensorReading
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 8:45
  * Version: 1.0
  */
object RichFunction {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(4)

    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")

    val sensorStream = source.map(
      line => {
        val fields = line.split(",")
        SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
      }
    )
    val res = sensorStream.flatMap(new MyFlatMap)

    res.print().setParallelism(1)
    //启动
    env.execute("RichFunction")
  }
}

//自定义flatMap类 输出每个元素，被处理时，所在的taskIndex
class MyFlatMap extends RichFlatMapFunction[SensorReading, (Int, SensorReading)] {
  var subTaskIndex = 0

  override def close(): Unit = super.close()

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def flatMap(t: SensorReading, collector: Collector[(Int, SensorReading)]): Unit = {
    collector.collect((subTaskIndex, t))

  }
}