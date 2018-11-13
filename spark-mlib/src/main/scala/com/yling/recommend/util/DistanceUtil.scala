package com.yling.recommend.util

import scala.math._

/**
 * Created by administrator on 2018/5/24.
 */
class DistanceUtil {

}

object DistanceUtil{

  def getTwoLocationDistance(srcLocationLat:Double,srcLocationLang:Double,desLocationLat:Double,desLocationLang:Double):Double={
    var distance: Double = 0
    var result: Double = 0
    distance = math.pow(srcLocationLat - desLocationLat,2)+math.pow(srcLocationLang-desLocationLang,2)
    result = 1/(1+distance)
    result
  }
}
