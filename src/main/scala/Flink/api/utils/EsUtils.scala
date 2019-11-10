package Flink.api.utils

import java.util

import Flink.api.SourceTest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 15:10:51
  */
object EsUtils {
  val httpHosts = new util.ArrayList[HttpHost]()
  //HttpHost(hostname: String, port: Int, scheme: String)
  httpHosts.add(new HttpHost("localhost",9200,"http"))

  //创建一个es sink的builder

  def getEsSinkBuilder(indexName:String) : ElasticsearchSink[SensorReading] = {
    val esFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //准备要写入的数据
        val json = new util.HashMap[String, String]()
        json.put("sensor_id", t.id)
        json.put("timestamp", t.timestamp.toString)
        json.put("temperature", t.temperature.toString)


        //创建 index request
        val indexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(json)

        //发出http请求
        //add(actionRequests: ActionRequest*)
        requestIndexer.add(indexRequest)
        println("save data " + t + " successfully")

      }
    }


    //Builder(httpHosts: List[HttpHost], elasticsearchSinkFunction: ElasticsearchSinkFunction[T])
    val sinkBuilder = new ElasticsearchSink.Builder[SensorReading](httpHosts,esFunc)

    //刷新前缓冲的最大动作量
    sinkBuilder.setBulkFlushMaxActions(10)

    sinkBuilder.build()
  }


}








