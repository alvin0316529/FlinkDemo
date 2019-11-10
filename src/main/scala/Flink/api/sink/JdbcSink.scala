package Flink.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import Flink.api.SourceTest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @Description
  * @Author alvin
  * @Date 2019-11-10 15:55:50
  */
class JdbcSink(sql:String) extends RichSinkFunction[SensorReading]{

  //定义连接、PreparedStatement
  //val dirver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/dbname?useSSL=false"
  val username = "root"
  val password = "123456"
  //val maxActive = "20"



  var conn : Connection = _
  var stmt : PreparedStatement = _

  //在初始化的过程中创建连接
  override def open(parameters: Configuration): Unit = {

    //创建连接
    conn = DriverManager.getConnection(url,username,password)

    stmt = conn.prepareStatement(sql)
  }

  //对每一条数据，调用连接执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //super.invoke(value,context)

    //执行
    stmt.setDouble(1,value.temperature)
    stmt.setString(2,value.id)

    stmt.execute()
  }

  override def close(): Unit = {
    stmt.close()
    conn.close()
  }
}


















