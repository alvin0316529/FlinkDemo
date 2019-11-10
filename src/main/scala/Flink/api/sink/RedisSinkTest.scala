package Flink.api.sink

import Flink.api.SourceTest.SensorReading
import Flink.api.utils.RedisUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 14:32:23
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val dataStream : DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })


    //sink
    //RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper)
    dataStream.addSink(RedisUtils.getRediSink())

    dataStream.print()

    env.execute("redis sink test")
  }
}
