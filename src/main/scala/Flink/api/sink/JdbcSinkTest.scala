package Flink.api.sink

import Flink.api.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 15:53:37
  */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val dataStream : DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    //sink
    val sql = "INSERT INTO tempareture(sensor,temp) VALUES(?,?)"
    dataStream.addSink(new JdbcSink(sql))


    dataStream.print()

    env.execute("mysql sink test")
  }
}
