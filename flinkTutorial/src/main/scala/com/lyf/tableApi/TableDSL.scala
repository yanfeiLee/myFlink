package com.lyf.tableApi

import com.lyf.beans.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/19 22:35
  * Version: 1.0
  */
object TableDSL {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.socketTextStream("hadoop104", 3333)
    val sensorStream = source.map(line => {
      val fields = line.split(",")
      SensorReading(fields(0).trim, fields(1).trim.toLong, fields(2).trim.toDouble)
    })

    //创建table环境
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,settings)

    //基于流创建表
    val table = tableEnv.fromDataStream(sensorStream)

    //表操作
    val res = table.select("id,temperature")
      .filter("id='sensor_1'")


    //table转为流输出结果
    val resStream = res.toAppendStream[(String,Double)]

    resStream.print().setParallelism(1)
     //启动
     env.execute("DSL")
  }
}
