package com.spark.recommendation.utils

import java.text.SimpleDateFormat
import java.util.Date

object TimeUtil {

  val now = new Date()

  def getCurrentTime(): Long = {
    val a = now.getTime
    val str = a+""
    str.substring(0,10).toLong
  }

  def get24HoursAgoTime(): Long = {
    val a = now.getTime
    val str = a + ""
    str.substring(0,10).toLong - 24 * 60 * 60
  }

  def get10MinutesAgoTime(): String = {
    val a = now.getTime
    val str = a + ""
    (str.substring(0,10).toLong - 10 * 60).toString
  }

  def getZeroTime():String={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    val str = a+""
    str.substring(0,10)
  }



}
