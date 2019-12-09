package com.zpark.stu.daomain

/**
  * 最终清洗的数据结果
  * @param ip 访问的ip地址
  * @param date 访问日期
  * @param time 访问时间
  * @param num  访问次数
  */
case class LastCleanoutCountDemo (ip: String, date: String, time: String, num: Int)
