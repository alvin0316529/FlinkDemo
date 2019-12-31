package UserTrack.com.qfh.hotitems

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

/**
  * @Description
  * @Author alvin
  * @Date 2019-12-30 15:11:07
  */
object KakfaConsumer {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    val brokerlist = "slave01.bigdata:9092,slave02.bigdata:9092,slave04.bigdata:9092"
    prop.setProperty("bootstrap.servers",brokerlist)
    prop.setProperty("group.id","group04")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset","earliest")

    val topic  = "hotitems"

    val consumer = new KafkaConsumer[String,String](prop)
    consumer.subscribe(Collections.singletonList(topic))

    var i = 0
    while(true){

      val records = consumer.poll(1000)
      val itr = records.records(topic).iterator()
      while(itr.hasNext){
        val data = itr.next()
        i = i + 1
        println("data" + i + ":" + data.key() + "--" + data.partition() + "--" + data.value())

      }
    }




  }
}

















































