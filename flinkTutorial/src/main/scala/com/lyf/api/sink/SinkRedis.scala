package com.lyf.api.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Project: myFlink
  * Create by lyf3312 on 20/03/20 22:10
  * Version: 1.0
  */
object SinkRedis {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\myFlink\\flinkTutorial\\src\\main\\resources\\sensor.txt")

    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop104").setPort(6379).build()
    source.addSink(new RedisSink[String](conf,new MyRedisMapper()))

     //启动
     env.execute("SinkRedis")
  }
}

class MyRedisMapper extends RedisMapper[String]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  override def getValueFromData(t: String): String = t.split(",")(2)

  override def getKeyFromData(t: String): String = t.split(",")(0)
}
