package LoginFailDetect


import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-17 16:40:55
  */

//输入的登陆事件样例类
case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)


//输出的报警信息样例类
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)



object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginEventStream = env.fromCollection( List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.3", "success", 1558430845),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ) )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
      } )
      .keyBy(_.userId)
      .process( new LoginWarning() )
      .print()

    env.execute("login fail job")

  }
}

//K, I, O
class LoginWarning() extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义保存登陆失败事件的状态
  lazy val loginFailState : ListState[LoginEvent] = getRuntimeContext.getListState[LoginEvent](new ListStateDescriptor[LoginEvent]("loginfail",classOf[LoginEvent]))

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context,
                              out: Collector[Warning]): Unit = {
//    //判断当前登陆状态是否为fail
//    if(value.eventType == "fail"){
//      //把新的失败时间添加到state
//      loginFailState.add(value)
//
//      ctx.timerService().registerEventTimeTimer((value.eventTime + 2) * 1000)
//    }else{
//      //如果登陆成功
//      loginFailState.clear()
//    }

    if(value.eventType == "fail"){
      //先获取之前失败的事件
      val iter = loginFailState.get().iterator()
      //如果之前已经有失败的事件，就做判断，如果没有，就把当前失败事件保存进State
      if(iter.hasNext){
        val firstFailEvent = iter.next()
        //判断两次失败时间间隔小于2秒，输出报警信息
        if(value.eventTime < firstFailEvent.eventTime + 2){
          out.collect(Warning(value.userId,
            firstFailEvent.eventTime,
            value.eventTime,
          "在2秒内连续两次登陆失败。"))
        }

        loginFailState.clear()
        //把最近一次登陆失败保存到state
        loginFailState.add(value)
      }else{
        loginFailState.add(value)
      }


    }else{
      loginFailState.clear()
    }


  }

//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    // 先把状态中的数据取出
//    val allLoginFailEvents: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//
//    val iter = loginFailState.get().iterator()
//    while( iter.hasNext )
//      allLoginFailEvents += iter.next()
//
//    // 判断登录失败事件个数，如果大于等于2，输出报警信息
//    if ( allLoginFailEvents.length >= 2 ){
//      out.collect( Warning( allLoginFailEvents.head.userId,
//        allLoginFailEvents.head.eventTime,
//        allLoginFailEvents.last.eventTime,
//        "在2秒之内连续登录失败" + allLoginFailEvents.length + "次。"
//      ) )
//    }
//    // 清空状态，重新开始计数
//    loginFailState.clear()
//  }
}







