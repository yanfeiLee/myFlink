package com.lyf.wc

import org.apache.flink.api.scala._
object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\word.txt")

    val res = ds.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    res.print()
  }
}
