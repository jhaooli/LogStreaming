package com.chen.Utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {

  val originalDate = FastDateFormat.getInstance("yyyy-MM-dd-HH:mm:ss")

  val targetDate = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time : String) = {
    originalDate.parse(time).getTime
  }

  def parseTime(time : String) = {
    targetDate.format(new Date(getTime(time))).toLong
  }

  def main(args: Array[String]): Unit = {
    val infos3 = new String("[2017-11-11")
    val infos4 = new String("06:22:22]")
    val time= infos3.substring(1,infos3.length)+"-"+infos4.substring(0,infos4.length-1)
    //print(parseTime(time))
    //print(parseTime(infos3+"-"+infos4))
  }

}
