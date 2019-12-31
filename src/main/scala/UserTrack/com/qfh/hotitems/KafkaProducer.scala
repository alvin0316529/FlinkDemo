package UserTrack.com.qfh.hotitems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.elasticsearch.common.recycler.Recycler.V

/**
  * 模拟kafka数据，先从文件中将数据写到kakfa
  *
  * @Description
  * @Author alvin
  * @Date 2019-11-16 23:41:28
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val topic = "hotitems"
    writeToKafka(topic)
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    val brokers = "slave01.bigdata:9092,slave02.bigdata:9092"
    properties.setProperty("bootstrap.servers",brokers)
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String,String](properties)

    val bufferedSource = io.Source.fromFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\UserBehavior.csv")

    println("总共读取 " + bufferedSource.size + " 条数据...")

    for(line <- bufferedSource.getLines()){
      val lineArrs = line.split(",")
      val key = lineArrs(0) + lineArrs(1) + lineArrs(4)

      val record = new ProducerRecord[String,String](topic,key,line)

      producer.send(record)
    }


    producer.close()

  }

}
