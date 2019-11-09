package Flink

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "F:\\data\\spark\\spark1.txt"

    //从文件中读取数据
    val inputDataSet = env.readTextFile(inputPath)

    //对dataset进行word count 处理
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map(word => (word,1))
      .groupBy(0)
      .sum(1)


    //输出
    wordCountDataSet.print()
  }
}
