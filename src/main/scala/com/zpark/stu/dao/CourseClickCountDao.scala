package com.zpark.stu.dao

import com.zpark.stu.daomain.LastCleanoutCountDemo
import com.zpark.stu.utils.HBaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 用户点击数统计访问层
  */
object CourseClickCountDao {

  val tableName = "MyJob"  //表名
  val cf = "info"   //列族
  val qualifer = "click_count"   //列

  /**
    * 保存数据到Hbase
    * @param list (ip: String, date: String, time: String, num: Int)
    */
  def save(list:ListBuffer[LastCleanoutCountDemo]): Unit = {
    //调用HBaseUtils的方法，获得HBase表实例
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(item <- list){
      //调用Hbase的一个自增加方法
      table.incrementColumnValue(Bytes.toBytes(item.ip+" "+item.date+" "+item.time),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        item.num)  //赋值为Long,自动转换
    }
  }
}

