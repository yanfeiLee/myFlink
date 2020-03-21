package com.lyf.processFunctionApi

import com.lyf.beans.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/17 21:53
  * Version: 1.0
  */
object KeyedPF {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500L)

    val source = env.socketTextStream("hadoop104", 3333)
    val sensorStream = source.map(line => {
      val fields = line.split(",")
      SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
    }).assignAscendingTimestamps(_.timestamp*1000L)

    val keyStream = sensorStream.keyBy(_.id)

    //如果温度值在3秒钟之内(processing time)连续上升，则报警。
    val res = keyStream.process(new MyKeyedProcessFun(3000L))

    res.print().setParallelism(1)
    //启动
    env.execute("KeyedPF")
  }

  class MyKeyedProcessFun(delay: Long) extends KeyedProcessFunction[String, SensorReading, String] {

    //定义状态值
    //上次温度
    lazy val lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double], -273.15))
    //定时器
    lazy val timer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-timer", classOf[Long], 0L))

    //处理来的每个元素
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      val currTemp = value.temperature
      val previouTemp = lastTemp.value()
      val timerTs = timer.value()
      //温度上升且没有定时器设定
      if (currTemp - previouTemp > 0 && timerTs == 0) {
        //获取当前元素，事件时间
        val currTs = ctx.timerService().currentWatermark()
        val triggerTs = currTs + delay
        //注册定时器,连续上升delay后，触发定时器
        ctx.timerService().registerEventTimeTimer(triggerTs)

        //更新状态变量timer
        timer.update(triggerTs)
      } else if (currTemp - previouTemp < 0 && timerTs != 0) {
        //清除定时器
        ctx.timerService().deleteEventTimeTimer(timerTs)
        //删除状态变量
        timer.clear()
      }else{

      }
      //更新lastTemp
      lastTemp.update(currTemp)
    }

    //定时器触发 后执行的操作
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //业务处理
      out.collect("传感器"+ctx.getCurrentKey+"温度持续上升"+delay/1000+"s")
      //清除定时器状态变量
      timer.clear()
    }
  }

}
