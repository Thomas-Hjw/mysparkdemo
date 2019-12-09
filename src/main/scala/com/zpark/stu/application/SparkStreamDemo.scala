package com.zpark.stu.application

import com.zpark.stu.dao.CourseClickCountDao
import com.zpark.stu.daomain.LastCleanoutCountDemo
import com.zpark.stu.utils.KafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreamDemo {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamDemo")
    val streamingContext = new StreamingContext(conf, Seconds(6))

    val kafkaUtil = new KafkaUtil
    val kafkavalue: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafka(streamingContext, "first", "spark")
    streamingContext.checkpoint("E:\\hadoop\\spark\\WordCount\\myspark")

    val updateFunc = (curVal: Seq[Int], preVal: Option[Int]) => {
      //进行数据统计当前值加上之前的值
      var total = curVal.sum
      //最初的值应该是0
      var previous = preVal.getOrElse(0)
      //Some 代表最终的但会值
      Some(total + previous)
    }

    val value: DStream[LastCleanoutCountDemo] = kafkavalue.map(_.value()).map(values => {
      val firstSplit: Array[String] = values.split(",")
      val twoSplit: Array[String] = firstSplit(1).split("T")
      val streeSplit: Array[String] = twoSplit(1).split("\\+")
      ((firstSplit(0), twoSplit(0), streeSplit(0)), 1)
    }).reduceByKey(_ + _).updateStateByKey(updateFunc).map(rdd => {
      LastCleanoutCountDemo(rdd._1._1, rdd._1._2, rdd._1._3, rdd._2)
    })

    value.print()

    value.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: ListBuffer[LastCleanoutCountDemo] = new ListBuffer[LastCleanoutCountDemo]
        partition.foreach(item => {
          list.append(LastCleanoutCountDemo(item.ip, item.date, item.time, item.num))
//          println(list)
        })
        CourseClickCountDao.save(list)
      })
    })
//    val list: ListBuffer[LastCleanoutCountDemo] = new ListBuffer[LastCleanoutCountDemo]
//    list.toList
//    CourseClickCountDao.save(list)



    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
