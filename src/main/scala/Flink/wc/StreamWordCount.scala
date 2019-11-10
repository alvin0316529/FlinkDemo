package Flink.wc

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

    //返回本地执行环境，需要在调用时指定默认的并行度
    //val env = StreamExecutionEnvironment.createLocalEnvironment(1)

    //返回集群执行环境，将JAR提交到远程服务器。需要在调用时指定JobManager的IP和端口号，并指定要在集群中执行的jar包
    //val env = StreamExecutionEnvironment.createRemoteEnvironment(host,port)

    //全局禁用chaining
    //env.disableOperatorChaining()



    //从socket读取数据
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
