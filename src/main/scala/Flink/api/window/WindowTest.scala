package Flink.api.window

import Flink.api.SourceTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}


/**
  * @Description
  * @Author alvin
  * @Date 2019-11-11 22:04:08
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)


    //val stream = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\sensor.txt")

    val stream = env.socketTextStream("localhost",7777)

    //val xx = new TimeWindow(1000L,1500L)
    //val yy = new GlobalWindow
    //val zz = new SlidingProcessingTimeWindows



    val dataStream : DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
      //.assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new MyAssigner())



    //统计10秒内的最小温度
    val minTempWindowStream = dataStream.map( data => (data.id,data.temperature))
        .keyBy(_._1)
        .timeWindow(Time.seconds(10))  //开时间窗口
        .reduce((data1,data2) => (data1._1,data1._2.min(data2._2)))  //用reduce做增量聚合


    minTempWindowStream.print("min temperature")
    dataStream.print("input data")

    env.execute()
  }



}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  val bound = 60000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000
  }
}
