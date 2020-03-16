package com.lyf.api.window

import com.lyf.beans.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 21:50
  * Version: 1.0
  */
object CustomWindowFunction {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")
    val sensorStream = source
      .map(
        line => {
          Thread.sleep(2000L) //减缓每条数据的处理时间，使其达到窗口步长
          val fields = line.split(",")
          SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
        }
      )
    //自定义窗口函数
    val res = sensorStream.keyBy("id")
      //指定窗口分配器为滚动窗口
      .timeWindow(Time.seconds(10))
      .apply(new MyWindowFun)

    res.print().setParallelism(1)

    //启动
    env.execute("CustomWindowFunction")
  }
}

class MyWindowFun extends WindowFunction[SensorReading, String, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[String]): Unit = {
    out.collect(key + "---" + window.getStart)
  }
}