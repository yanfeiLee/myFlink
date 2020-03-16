package com.lyf.api.window

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 15:38
  * Version: 1.0
  */
object MyWaterMark {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置watermark周期,默认200ms
    env.getConfig.setAutoWatermarkInterval(300L)

    val source = env.socketTextStream("hadoop104",3333)
    val sensorStream = source
      .map(
        line => {
          val fields = line.split(",")
          SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
        }
      )

    //设置watermark并有1s延时
    val watermarkStream = sensorStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = {
        //转为ms为单位的时间戳
        element.timestamp * 1000L
      }
    })


    //watermark没有延时,即数据没有乱序
    sensorStream.assignAscendingTimestamps(_.timestamp * 1000L)

    //自定义watermark
    sensorStream.assignTimestampsAndWatermarks(new MyPeriodicWaterMarkAssign(1000L))


    val res = watermarkStream.keyBy(1)
      .timeWindow(Time.seconds(10)) //开一个10s的滚动窗口
      .reduce(
      (curSensor, newSensor) => {
        SensorReading(curSensor.id, newSensor.timestamp, curSensor.temperature.min(newSensor.temperature))
      })

    res.print().setParallelism(1)
    //启动
    env.execute("MyWaterMark")
  }
}

//自定义周期性生成watermark的assiger
class MyPeriodicWaterMarkAssign(delay: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {

  // 初始化观察到的最大时间戳
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - delay)

  //根据时间时间，提取当前最大时间戳
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

//自定义可以间断地生成watermark的 assiger
class MyPunctuatedWaterMarkAssign extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    //id为3的sensor生成watermark
    if (lastElement.id == "sensor_3") {
      new Watermark(extractedTimestamp)
    } else {
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}
