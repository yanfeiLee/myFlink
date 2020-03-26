package com.lyf.tableApi.udf

import com.lyf.api.source.MySource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/25 12:03
  * Version: 1.0
  */
object MyScalarFunction {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val source = env.addSource(new MySource)

    //创建table环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val hash = new MyHash(10)
    tEnv.fromDataStream(source)
      .select('id, hash('id))
      .toAppendStream[(String, Long)]
      .print()





    //启动
    env.execute("ScalarFunction")
  }

  class MyHash(factor: Int) extends ScalarFunction {

    def eval(s: String): Long = {
      s.hashCode * factor
    }
  }

}
