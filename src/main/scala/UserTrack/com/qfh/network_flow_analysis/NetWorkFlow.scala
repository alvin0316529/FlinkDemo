package UserTrack.com.qfh.network_flow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @Description 网络流量数据分析
  * @Author alvin
  * @Date 2019-11-17 10:03:05
  */


//输入log数据样例类
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

//中间统计结果样例类
case class UrlViewCount(url:String,windowEnd:Long,count:Long)


object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\apache.log")

    val dataStream = stream//.filter(_.contains(".html"))
      .map(data => {
      val dataArray = data.split("\\s")
      //17/05/2015:10:05:03
      val simpleDate = new SimpleDateFormat("dd/MM/yyyy:hh:mm:ss")
      val timestamp = simpleDate.parse(dataArray(3).trim).getTime
      ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
    })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      } )
      .keyBy(_.url)
      .timeWindow(Time.minutes(1),Time.seconds(20))
      .aggregate(new CountUrlAgg(),new WindowUrlResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()


    env.execute("net work flow")

  }
}


class CountUrlAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def merge(a: Long, b: Long): Long = a + b

  override def getResult(accumulator: Long): Long = accumulator


}


//IN, OUT, KEY, W
class WindowUrlResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}


//<K, I, O>
class TopNHotUrls(nSize:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  private var urlState:ListState[UrlViewCount] = _


  override def open(parameters: Configuration): Unit = {
    urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))
  }

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.add(value)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allUrlViews : ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()

    val iter = urlState.get().iterator()
    while(iter.hasNext)
      allUrlViews += iter.next()

    urlState.clear()


    //按照count大小进行排序
    val sortedUrlViews = allUrlViews.sortWith((left,right) => left.count > right.count).take(nSize)


    //将数据排名信息格式化成string，方便打印输出
    val result : StringBuilder = new StringBuilder()

    result.append("=========================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")


    for(i <- sortedUrlViews.indices){
      val currentUrlView : UrlViewCount = sortedUrlViews(i)

      result.append("No").append(i+1).append(": ")
        .append(" URL=").append(currentUrlView.url)
        .append(" 流量=").append(currentUrlView.count)
        .append("\n")
    }

    result.append("=======================\n")

    Thread.sleep(1000)
    out.collect(result.toString())

  }

}


