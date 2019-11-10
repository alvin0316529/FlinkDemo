package Flink.api.sink

import Flink.api.SourceTest.SensorReading
import Flink.api.utils.EsUtils
import org.apache.flink.streaming.api.scala._

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 15:02:02
  */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val dataStream : DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    //sink
    dataStream.addSink(EsUtils.getEsSinkBuilder("sensor"))


    dataStream.print()

    env.execute("es sink test")
  }
}
