package Flink.api

import Flink.api.SourceTest.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 09:41:15
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)


    val streamFromFile = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    //map
    val dataStream : DataStream[SensorReading] = streamFromFile.map( data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    }).keyBy("id")
        //.sum("temperature")

    //输出当前传感器最新的温度+10，而且时间戳是上一次数据的时间+1
        .reduce((x,y) => SensorReading(x.id,x.timestamp + 1,y.temperature + 10))

    //2.多流转换算子
    val splitStream = dataStream.split( data => {
      if(data.temperature > 30) Seq("high") else Seq("low")
    })

    val high : DataStream[SensorReading] = splitStream.select("high")
    val low : DataStream[SensorReading]  = splitStream.select("low")
    val all : DataStream[SensorReading] = splitStream.select("high","low")


    //合并两条流
    val warning : DataStream[(String,Double)] = high.map(data => (data.id,data.temperature))

    //: ConnectedStreams[T, T2]
    val connectedStream : ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)

    val coMapDataStream : DataStream[Product with Serializable] = connectedStream.map(
      warningdData => (warningdData._1,warningdData._2,"warning"),
      lowData => (lowData.id,"healthy")
    )

    val unionDataStream : DataStream[SensorReading] = high.union(low)




    //print
    //dataStream.print().setParallelism(1)
    //high.print("high")
    //low.print("low")
    //all.print("all")
    //coMapDataStream.print()
    //unionDataStream.print()

    env.execute("TransformTest")
  }
}
