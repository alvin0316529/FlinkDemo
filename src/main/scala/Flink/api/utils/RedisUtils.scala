package Flink.api.utils

import Flink.api.SourceTest.SensorReading
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 14:40:58
  */
object RedisUtils {
  val conf = new FlinkJedisPoolConfig.Builder()
    .setHost("slave04.bigdata")
    .setPort(6379)
    .build()

  def getRediSink():RedisSink[SensorReading] = {
    //RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper)
    new RedisSink[SensorReading](conf,new MyRedisSink)
  }

  class MyRedisSink() extends RedisMapper[SensorReading]{

    //保存到redis的命令描述，HSET key field value
    override def getCommandDescription: RedisCommandDescription = {
      //RedisCommandDescription(redisCommand: RedisCommand, additionalKey: String)
      new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
    }

    override def getKeyFromData(data: SensorReading): String = data.id

    override def getValueFromData(data: SensorReading): String = data.temperature.toString
  }

}
