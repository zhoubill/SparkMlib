package com.yling.recommend.util

import java.util
import java.util.Comparator

/**
 * Created by zhouzhou on 2018/5/28.
 */
class MapValueComparator extends Comparator[util.Map.Entry[String,Double]]{

    def compare(me1:util.Map.Entry[String,Double],me2:util.Map.Entry[String,Double]):Int={
     me1.getValue.compareTo(me2.getValue)
   }
}
