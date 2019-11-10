package Flink.api

import Flink.api.SourceTest.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 09:05:29
  */
class SensorSource extends SourceFunction[SensorReading]{

  //定义一个flag，表示数据源是否正常运行
  var running: Boolean = true


  //取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }

  //正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val rand = new Random()

    //初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i,60 + rand.nextGaussian() * 20)
    )

    //用无限循环，产生数据流
    while(running){
      //在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1,t._2 + rand.nextGaussian())
      )

      //获取当前时间戳
      val curTime = System.currentTimeMillis()

      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1,curTime,t._2))
      )

      //设置时间间隔
      Thread.sleep(500)

    }

  }


}
