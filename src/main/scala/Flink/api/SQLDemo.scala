package Flink.api

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * @Description
  * @Author alvin
  * @Date 2019-12-27 14:27:43
  */
object SQLDemo {
  case class PlayerData(season:String,player:String,play_num:String,
                        first_court:Int,time:Double,assists:Double,
                        steals:Double,blocks:Double,scores:Double)

  case class PlayerResult(player:String,num:Long)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.readTextFile("F:\\workspace\\FlinkDemo\\src\\main\\resources\\score")

    val topInput = input.map( data => {
      val dataArray = data.split(",")
      PlayerData(dataArray(0).trim,dataArray(1).trim,dataArray(2).trim,
        dataArray(3).trim.toInt,dataArray(4).trim.toDouble,dataArray(5).trim.toDouble,
        dataArray(6).trim.toDouble,dataArray(7).trim.toDouble,dataArray(8).trim.toDouble
      )
    })

    val topScore = tableEnv.fromDataSet(topInput)

    tableEnv.registerTable("score",topScore)

    val queryResult = tableEnv.sqlQuery(
      """
        |select player,
        |count(season) as num
        |from score
        |group by player
        |order by num desc
        |limit 10
      """.stripMargin)

    val result = queryResult.toDataSet[PlayerResult]
    result.print()


    val queryResult2 = tableEnv.sqlQuery(
      """
        |select * from score order by season limit 20
      """.stripMargin)

    println("--------------------------")

    val result2 = queryResult2.toDataSet[PlayerData]

    result2.print()

  }
}


























