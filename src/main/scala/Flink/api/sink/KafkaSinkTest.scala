package Flink.api.sink

import java.util.Properties

import Flink.api.SourceTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 14:01:57
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers","localhost:9092")
    prop.setProperty("group.id","consume-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset","latest")

    val topic = "sensor"

    //source: read from kafka
    val stream = env.addSource(new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop))

    //这里将数据toString了
    val dataStream : DataStream[String] = stream.map( data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble).toString
    })

    //sink : consumer
    //public FlinkKafkaProducer011(String brokerList, String topicId, SerializationSchema<IN> serializationSchema)
    val brokerList = "localhost:9092"
    val topicId = "test"
    dataStream.addSink(new FlinkKafkaProducer011[String](brokerList,topicId,new SimpleStringSchema()))


    env.execute("kafka sink test")

  }
}
