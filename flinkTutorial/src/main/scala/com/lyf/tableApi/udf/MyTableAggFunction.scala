package com.lyf.tableApi.udf

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import java.lang.{Integer => JInteger}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.lang.{Iterable => JIterable}

import org.apache.flink.util.Collector
/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/26 10:04
  * Version: 1.0
  * 自定义函数求TopN
  */
object MyTableAggFunction {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val inputStream = env.fromElements(
      (1, -1),
      (1, 2),
      (1, 0),
      (1, 5),
      (1, 4)
    )
    val top2 = new Top2
    tEnv.fromDataStream(inputStream, 'k, 'v)
      .groupBy('k)
      .flatAggregate(top2('v) as('v, 'rk))
      .select('k, 'v, 'rk)
      .toRetractStream[(Long, Long, Int)]
      .print()


    //启动
    env.execute("MyTableAggFunction")
  }
}

class Top2Acc {
  var first: JInteger = _
  var second: JInteger = _
}

//T 聚合结果
//ACC 聚合中间值
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Acc] {
  //初始化中间聚合值
  override def createAccumulator(): Top2Acc = {
    val acc = new Top2Acc
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  //计算排名
  def accumulate(acc: Top2Acc, v: Int): Unit = {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    }else if( v > acc.second){
      acc.second = v
    }
  }

  //各分区进行聚合
  def merge(acc: Top2Acc, its: JIterable[Top2Acc]): Unit = {
    val top2AccIts = its.iterator()
    while (top2AccIts.hasNext){
      val top2 = top2AccIts.next()
      accumulate(acc,top2.first)
      accumulate(acc,top2.second)
    }
  }

  //将计算的结果输出
  def emitValue(acc: Top2Acc, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // emit the value and rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}


