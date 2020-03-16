package com.lyf.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


object WordCountStreamingDisableChain {

  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置各个操作的整体并行度
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")
    //获取流
    val ds = env.socketTextStream(host, port)

    //处理
    val keyByStream= ds
      .flatMap(_.split(" "))
      .filter(_.nonEmpty).disableChaining() //断开与前后算子的operator chain
      .map((_, 1))
      .keyBy(0)


    val res = keyByStream.sum(1)
    //输出
    res.print().setParallelism(1)


    //启动
    env.execute("Socket stream word count")


  }
}
