package com.lyf.api.source


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/15 21:46
  * Version: 1.0
  */
object SourceFile {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
     
    val res = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")
    
    res.print("source File").setParallelism(1)
     //启动
     env.execute("SourceFile")
  }
}
