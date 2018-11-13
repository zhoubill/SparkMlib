package com.yling.recommend.util

import java.util.Date

/**
 * Created by zhouzhou on 2018/5/25.
 */
class TimeIntervalUtil {

}

object TimeIntervalUtil{

  def getPercentOfTime(startDate1:Date,endDate1:Date, startDate2:Date,endDate2:Date):Double={
    var start :Double=0
    var end :Double=0
    var percent :Double=0
    val startTime1 = startDate1.getTime()
    val startTime2 = startDate2.getTime()
    val endTime1 = endDate1.getTime()
    val endTime2 = endDate2.getTime()
    if (startTime1 >= startTime2) {
      start = startTime1
    } else {
      start = startTime2
    }

    if (endTime1 >= endTime2) {
      end = endTime1
    } else {
      end = endTime2
    }
    //计算总的时间长度
     val allDays = (end - start) / (1000 * 60 * 60);
    //时间窗口2完全不在时间窗口1的时间范围区间之内
    if (endTime2 <= startTime1 || startTime2 >= endTime1) {
      percent=0
    }
    //时间窗口2的时间区间完全在时间窗口1的时间范围内
    if (startTime2 >= startTime1 && endTime2 <= endTime1) {
      percent = 1
    }
    //时间窗口2的开始时间在时间窗口1的范围之内并且时间窗口2的结束时间大于时间窗口1的结束时间
    if (startTime1 < startTime2 && startTime2 < endTime1 && endTime2 > endTime1) {
      val days = (endTime1 - startTime2) / (1000 * 60 * 60);
      percent = days / allDays;
    }
    //时间窗口2的结束时间在时间窗口1的范围内并且时间窗口2的开始时间大于时间窗口1的时间
    if (endTime2 < endTime1 && endTime2 > startTime1 && startTime2 < startTime1) {
      val days = (endTime2 - startTime1) / (1000 * 60 * 60);
      percent = days / allDays;
    }

    percent
  }
}
