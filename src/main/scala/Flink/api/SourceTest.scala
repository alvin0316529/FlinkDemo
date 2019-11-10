package Flink.api


import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @Description 从集合读取数据
  * @Author alvin
  * @Date 2019-11-09 17:26:20
  */




object SourceTest {
  //温度传感器读数样例类
  case class SensorReading(id:String,timestamp:Long,temperature:Double)

  def main(args: Array[String]): Unit = {
    val env  = StreamExecutionEnvironment.getExecutionEnvironment


    //1.自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1",1547718199,32.78),
      SensorReading("sensor_6",1547718255,39.78),
      SensorReading("sensor_7",1547718698,28.78),
      SensorReading("sensor_10",1547719532,25.75)
    ))


    //2.从文件中读取数据
    val stream2 = env.readTextFile("F:\\data\\spark\\spark1.txt")



    //3.从kafka读取数据
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","localhost:9092")
    prop.setProperty("group.id","consume-group")
    prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("auto.offset.reset","latest")

    val topic = "sensor"

    //FlinkKafkaConsumer011(topic: String, valueDeserializer: DeserializationSchema[T], props: Properties)
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop))



    //4.自定义Source
    val stream4 = env.addSource(new SensorSource())


    //stream1.print("stream1").setParallelism(1)
    //stream2.print("stream2").setParallelism(1)
    //stream3.print("stream3").setParallelism(1)
    stream4.print("stream4").setParallelism(1)

    env.execute("source test")
  }

}








