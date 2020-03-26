package com.lyf.tableApi

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Tumble}
import org.apache.flink.table.api.scala._

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/25 9:17
  * Version: 1.0
  */
object BlinkTopN {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //创建blink 配置
    val settting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    //创建表环境
    val tEnv = StreamTableEnvironment.create(env, settting)

    //利用表操作实现topN
    val url = getClass.getResource("/UserBehavior.csv")
    val userBehaviorStream = env.readTextFile(url.getPath)
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)

    //'timestamp.rowTime 表示事件时间，
    val table = tEnv.fromDataStream(userBehaviorStream, 'timestamp.rowtime, 'itemId)

    val t = table
      //      .window(Slide over 60.minutes every 5.minutes on 'timestamp as 'w) // 1小时步长为5min的滑动窗口
      .window(Tumble over 1.hour on 'timestamp as 'w) //1小时的滚动窗口
      .groupBy('itemId, 'w)
      .aggregate('itemId.count as 'icount)
      .select('itemId, 'icount, 'w.end as 'windowEnd) //'w.end 窗口的结束时间
      .toAppendStream[(Long, Long, Timestamp)]


    //利用sql选择topN：Blink的新增特性
    tEnv.createTemporaryView("topn", t, 'itemId, 'icount, 'windowEnd)

    tEnv.sqlQuery(
      """
        |select *
        |from (
        |         select *,row_number() over (partition by windowEnd order by icount desc) as row_num
        |         from topn
        |     )
        |where row_num < 5
      """.stripMargin)
      .toRetractStream[(Long, Long, Timestamp, Long)]
      .filter(_._1) //过滤掉 删除数据的输出 即_._1 为false的输出
      .print()
    //启动
    env.execute("BlinkTopN")
  }
}
