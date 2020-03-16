package com.lyf.api.udf

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/16 8:38
  * Version: 1.0
  */
object FilterUDF {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\word.txt")
    val filterStream = source.flatMap(_.split(","))
            .filter(new MyFilter("hello"))
      .filter(
      //匿名函数
        new FilterFunction[String] {
          override def filter(t: String): Boolean = {
            t.contains("abc")
          }
        }
    )
    filterStream.print().setParallelism(1)

    //启动
    env.execute("FilterUDF")
  }
}

class MyFilter(keyword:String) extends FilterFunction[String] {
  override def filter(t: String): Boolean = {
    t.contains(keyword)
  }
}