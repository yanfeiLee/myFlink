package com.lyf.tableApi.udf

import javax.swing.JToolBar.Separator
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/25 13:50
  * Version: 1.0
  */
object MyTableFunction {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val data = env.fromElements(
      "hello#worlds",
      "nihao#beijing"
    )
    val data2 = env.fromElements(
      "hello2#worlds2",
      "nihao2#beijing2"
    )
    val table = tEnv.fromDataStream(data, 's)
    val table2 = tEnv.fromDataStream(data2, 's2)
    val split = new Split("#")

    //    table2.joinLateral(split('s2) as('word, 'length))
    //      .select('s, 'word, 'length)
    //      .toAppendStream[(String, String, Long)]
    //      .print()
    //    table.leftOuterJoinLateral(split('s) as('w, 'l))
    //        .select('s,'w,'l)
    //        .toAppendStream[(String,String,Long)]
    //        .print()


    //sql风格
    //注册自定义函数
    tEnv.registerFunction("split", new Split("#"))
    tEnv.createTemporaryView("t", table, 's) //创建临时表
    //    tEnv.sqlQuery(
    //      """
    //        |select s,word,length
    //        |from t,lateral table (split(s)) as T(word,length)
    //      """.stripMargin)
    //        .toAppendStream[(String,String,Long)]
    //        .print()

    //left join
    val queryRes = tEnv.sqlQuery(
      """
select s,w,l
from t left join lateral table (split(s)) as T(w,l) on true
      """.stripMargin)
    val res = queryRes.toAppendStream[(String, String, Long)]
    res.print()


    //启动
    env.execute("MyTableFunction")
  }

  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      // use collect(...) to emit a row.
      str.split(separator).foreach(x => collect((x, x.length)))
    }
  }

}
