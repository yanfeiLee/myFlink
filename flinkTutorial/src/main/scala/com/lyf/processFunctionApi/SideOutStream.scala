package com.lyf.processFunctionApi

import com.lyf.beans.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/19 21:03
  * Version: 1.0
  */
object SideOutStream {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置状态后端得类型，默认为MemoryStateBackend
    env.setStateBackend(new FsStateBackend("hdfs://hadoop104:9000/flinkBackend"))
//      env.setStateBackend(new RocksDBStateBackend("连接rocksDB得uri"))

    // checkpoint配置
    env.enableCheckpointing(10000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    val source = env.socketTextStream("hadoop104", 3333)
    val sensorStream = source.map(line => {
      val fields = line.split(",")
      SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
    })

    val splitStream = sensorStream.process(new SplitSensor(30))

    val res = splitStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-stream"))

    res.print().setParallelism(1)

     //启动
     env.execute("SideOutStream")
    
  }
}

class SplitSensor(threshold: Double) extends ProcessFunction[SensorReading,SensorReading]{

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < threshold){
        //低于阈值输出到测输出流
        ctx.output(new OutputTag[(String, Double, Long)]("low-stream"),(value.id,value.temperature,value.timestamp))
      }else{
         //正常输出
        out.collect(value)
      }
  }
}