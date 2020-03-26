package com.lyf.tableApi.udf

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/25 14:20
  * Version: 1.0
  */
object MyAggFunction {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val source = env.fromElements(
      (1, -1),
      (1, 1),
      (1, -3),
      (1, 4)
    )
    val agg = new MyAgg
    tEnv.fromDataStream(source, 'k, 'v)
      .groupBy('k)
      .aggregate(agg('v) as ('minV, 'maxV))
      .select('k, 'minV, 'maxV)
      .toRetractStream[(Long, Long, Long)]
      .filter(_._1) //只输出更新值，
      .print()


    //启动
    env.execute("MyAggFunction")
  }
}

case class MyAggRes(var minV: Int, var maxV: Int)

class MyAgg extends AggregateFunction[Row, MyAggRes] {

  override def getValue(acc: MyAggRes): Row = Row.of(Integer.valueOf(acc.minV),Integer.valueOf(acc.maxV))

  override def createAccumulator(): MyAggRes = MyAggRes(Int.MaxValue, Int.MinValue)

  override def getResultType: TypeInformation[Row] = new RowTypeInfo(Types.INT(), Types.INT())

  def resetAccumulator(acc: MyAggRes): Unit = {
    acc.maxV = Int.MinValue
    acc.minV =Int.MaxValue
  }

  def accumulate(acc: MyAggRes, value: Int) = {
    if (value > acc.maxV) acc.maxV = value
    if (value < acc.minV) acc.minV = value
  }
}
