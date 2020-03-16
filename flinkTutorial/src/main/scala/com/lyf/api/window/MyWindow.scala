package com.lyf.api.window

import com.lyf.beans.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 10:01
  * Version: 1.0
  */
object MyWindow {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置使用eventTime
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")
    val sensorStream = source
      .map(
        line => {
          Thread.sleep(2000L) //减缓每条数据的处理时间，使其达到窗口步长
          val fields = line.split(",")
          SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
        }
      )

    val windowedstream = sensorStream.keyBy("id")
//      .timeWindow(Time.seconds(13)) //滚动处理窗口
//      .timeWindow(Time.seconds(10),Time.seconds(1)) //滑动事件窗口
//           .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(1)))
//          .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
      //会话窗口,5s内没有数据传来，则算做一个session window
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
    val res = windowedstream.min(2)

    res.print().setParallelism(1)



    //启动
    env.execute("MyWindow")
  }
}

