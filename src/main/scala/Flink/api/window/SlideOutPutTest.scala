package Flink.api.window

import Flink.api.SourceTest.SensorReading
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-12 23:16:49
  */
object SlideOutPutTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置状态后端，一般不在代码里面设置，在配置文件里面设置
    //env.setStateBackend(new FsStateBackend(""))
    //env.setStateBackend(new MemoryStateBackend())



    val stream = env.socketTextStream("localhost",7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })

    val processedStream = dataStream.process(new FreezingAlert())

    // dataStream.print("input data")
    //打印主输出流
    processedStream.print("processed data")

    //打印侧输出流
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).printToErr("alert data")

    env.execute()

  }
}

/**
  * 冰点报警，如果小于32F,输出报警信息到侧输出流
  *
  * @param < I> Type of the input elements.
  * @param < O> Type of the output elements.
  */
class FreezingAlert extends ProcessFunction[SensorReading,SensorReading]{
  lazy val alertOutput : OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    if(value.temperature < 32.0){
      /**
        * Emits a record to the side output identified by the {@link OutputTag}.
        *
        * @param outputTag the { @code OutputTag} that identifies the side output to emit to.
        * @param value The record to emit.
        */

      ctx.output(alertOutput,"freezing alert for " + value.id)
    }else{

      //主输出流逻辑
      out.collect(value)
    }
  }
}




