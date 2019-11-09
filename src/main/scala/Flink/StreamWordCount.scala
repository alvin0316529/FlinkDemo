package Flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-09 15:44:20
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //从外部传人参数
    val params = ParameterTool.fromArgs(args)

    val host = params.get("host")
    val port = params.getInt("port")


    //创建上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * def socketTextStream(hostname: String, port: Int, delimiter: Char = '\n', maxRetry: Long = 0):
      * DataStream[String]
      */
    val textDataStream : DataStream[String] = env.socketTextStream(host,port)

    val wordCountDataStream = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map(word => (word,1))
      .keyBy(0)
      .sum(1)

    //默认的并行度是CPU的核数
    wordCountDataStream.print()

    //wordCountDataStream.print().setParallelism(1)

    //启动flink,执行任务
    env.execute("StreamWordCount")
  }
}
