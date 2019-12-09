package com.zpark.stu.application

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zpark.stu.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将处理后的数据保存到MySql数据库中
  */
object MySparkStreamingDemo {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MySparkStreamingDemo").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("E:\\hadoop\\spark\\WordCount\\myspark")
    val updateFunc = (curVal: Seq[Int], preVal: Option[Int]) => {
      //进行数据统计当前值加上之前的值
      var total = curVal.sum
      //最初的值应该是0
      var previous = preVal.getOrElse(0)
      //Some 代表最终的但会值
      Some(total + previous)
    }
    //获取kafka生成者中的数据1
//    val kafkaDs: InputDStream[ConsumerRecord[String, String]] = getKafka(ssc, "first", "spark")

    val kafkaUtil = new KafkaUtil()
    val kafkaDs: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafka(ssc,"first","spark")


    val values: DStream[(String, String, String, Int)] = kafkaDs.map(_.value()).map(x => {
      val ip: Array[String] = x.split(",")
      val date: Array[String] = ip(1).split("T")
      val time: Array[String] = date(1).split(":")
      ((ip(0), date(0), time(0)), 1)
    }).reduceByKey(_ + _).updateStateByKey(updateFunc).map(rdd => {
      (rdd._1._1, rdd._1._2, rdd._1._3, rdd._2)
    })
    values.print()

    values.foreachRDD(cs => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      try{

        Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
        cs.foreachPartition(f => {
          conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true", "root", "403411");
          ps = conn.prepareStatement("insert into firstjob values(?,?,?,?)")
          f.foreach(s => {
            if(s != null){
              ps.setString(1, s._1)
              ps.setString(2, s._2)
              ps.setString(3, s._3)
              ps.setInt(4, s._4)
              ps.executeUpdate()
            }
          })
        })
      }catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }finally {
        if (ps != null){
          ps.close()
        }
        if (conn != null){
          conn.close()
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
