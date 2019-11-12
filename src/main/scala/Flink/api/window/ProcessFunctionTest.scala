package Flink.api.window


import Flink.api.SourceTest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-12 21:42:59
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost",7777)

    val dataStream = stream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })


    val processedStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())


    dataStream.print("input data")

    processedStream.print("process data")

    env.execute("process function test")

  }
}


/**
  * KeyedProcessFunction[String,SensorReading,String]
  * @param < K> Type of the key.
  * @param < I> Type of the input elements.
  * @param < O> Type of the output elements.
  */
class TempIncreAlert() extends KeyedProcessFunction[String,SensorReading,String]{

  //定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))

  //定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()

    //更新温度值
    lastTemp.update(value.temperature)

    val curTimerTs = currentTimer.value()


    //温度上升且没有设过定时器，则注册定时器
    if(value.temperature > preTemp && curTimerTs == 0){
      //注册定时器
      val timerTs = ctx.timerService().currentProcessingTime() + 1000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)

    }else if(preTemp > value.temperature || preTemp == 0.0){
      //如果温度下降或者是第一条数据，删除定时器，并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }


  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    //输出报警信息
    out.collect(ctx.getCurrentKey + " 温度连续上升")
    currentTimer.clear()

  }
}