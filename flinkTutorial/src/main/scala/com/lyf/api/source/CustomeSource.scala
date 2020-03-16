package com.lyf.api.source

import java.util.Random

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/15 22:02
  * Version: 1.0
  */
object CustomeSource {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


     //读取数据源
      val res = env.addSource(new MySource)

      res.print("custom source").setParallelism(1)


     //启动
     env.execute("CustomeSource")
  }
}

class MySource extends SourceFunction[SensorReading]{

  // flag: 表示数据源是否还在正常运行
  var running  = true

  override def cancel(): Unit = running = false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 初始化一个随机数发生器
    val rand = new Random()

    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 65 + rand.nextGaussian() * 20 )
    )

    while(true){
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian() )
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }
}