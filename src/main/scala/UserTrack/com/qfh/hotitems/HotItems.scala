package UserTrack.com.qfh.hotitems


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @Description
  * @Author alvin  用户行为分析 - 热门商品统计
  * @Date 2019-11-16 18:31:27
  */

//输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

//中间输出的商品浏览量的样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)


object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置时间特性为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val stream = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\UserBehavior.csv")


    val prop = new Properties()
    val brokerlist = "slave01.bigdata:9092,slave02.bigdata:9092,slave04.bigdata:9092"
    prop.setProperty("bootstrap.servers",brokerlist)
    prop.setProperty("group.id","group1")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //prop.setProperty("auto.offset.reset","latest")
    prop.setProperty("auto.offset.reset","earliest")

    val topic  = "hotitems"

    val stream = env.addSource(new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop ))


    val dataStream : DataStream[ItemViewCount] = stream.map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,
        dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
    })

      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new CountAgg(),new WindowResult())


    //接下来需要根据windowEnd排序
    val resultStream : DataStream[String] = dataStream.keyBy(_.windowEnd)
        .process(new TopNHotItems(3))


      resultStream.print("hotItems")


    //preAggregator: AggregateFunction[T, ACC, V],
    //windowFunction: ProcessWindowFunction[V, R, K, W]





    env.execute("hot items")




  }
}


/**
  * 自定义预聚合函数，来一个数就加1
  * @param < IN>  The type of the values that are aggregated (input values)
  * @param < ACC> The type of the accumulator (intermediate aggregate state).
  * @param < OUT> The type of the aggregated result
  */
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def merge(a: Long, b: Long): Long = a + b

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

}


/**
  * 自定义求平均值
  */
class averageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double]{
  //初始化变量
  override def createAccumulator(): (Long, Int) = (0L,0)

  //分区数据聚合
  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1,a._2 + b._2)

  //数据聚合
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (value.timestamp + accumulator._1,accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._2 / accumulator._1

}

/**
  * 自定义窗口函数，包装成ItemViewCount输出
  * @tparam IN The type of the input value.
  * @tparam OUT The type of the output value.
  * @tparam KEY The type of the key.
  * @tparam W The type of the window.
  */
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId,windowEnd,count))
  }
}


/**
  * 自定义processFunction,排序处理数据
  * 根据windowEnd分组后，需要对每个windowEnd里面的ItemViewCount中的count值降序排列
  * @param < K> Type of the key.
  * @param < I> Type of the input elements.
  * @param < O> Type of the output elements.
  * @param nSize
  */
class TopNHotItems(nSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  //定义一个List state，用来保存所有的ItemViewCount
   private var itemState : ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor("itemState",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每一条数据都存入state中
    itemState.add(value)

    //注册windowEnd + 1 的EventTime Timer,当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    //也就是当程序看到windowEnd + 1 的水位线watermark时，触发onTimer回调函数
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)

  }

  //实现onTimer的回掉
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器触发时，已经收集到所有数据，首先把所有数据放到一个list中
    val allItems : ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()

    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }

    //清除状态，释放空间
    itemState.clear()

    //按照count大小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(nSize)


    //将数据排名信息格式化成string，方便打印输出
    val result : StringBuilder = new StringBuilder()

    result.append("=========================\n")
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")


    for(i <- sortedItems.indices){
      val currentItem : ItemViewCount = sortedItems(i)

      result.append("No").append(i+1).append(": ")
        .append("商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }

    result.append("=======================\n")

    Thread.sleep(1000)

    out.collect(result.toString())

  }
}















