package com.lyf.processFunctionApi

import com.lyf.beans.SensorReading
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/19 21:24
  * Version: 1.0
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.socketTextStream("hadoop104", 3333)
    val sensorStream = source.map(line => {
      val fields = line.split(",")
      SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
    })

    val keyedStream = sensorStream.keyBy(_.id)
    // 检测传感器温度，如果连续两次的温度差值超过10度，输出报警,输出（id, lastTemp, curTemp）
    val res = keyedStream
      //      .flatMap(new MyMapState(10.0))
      .flatMapWithState[(String, Double, Double), Double] {
        case (elem: SensorReading,None) => (List.empty, Some(elem.temperature))
        case (elem: SensorReading, lastTemp: Some[Double]) => {
          val diff = (elem.temperature - lastTemp.get).abs
          if (diff>10){
            (List((elem.id,lastTemp.get,elem.temperature)),Some(elem.temperature))
          }else{
            (List.empty,Some(elem.temperature))
          }
        }
    }

    res.print().setParallelism(1)

    //启动
    env.execute("StateTest")
  }
}

class MyMapState(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    val diff = (lastTemp - value.temperature).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature))
    }

    //更新状态
    lastTempState.update(value.temperature)
  }
}