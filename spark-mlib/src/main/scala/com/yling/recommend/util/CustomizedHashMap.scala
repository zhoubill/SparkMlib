package com.yling.recommend.util

import java.util

/**
 * Created by zhouzhou on 2018/5/25.
 */
class CustomizedHashMap [K, V] extends util.HashMap[K,V] {

  override  def toString():String={
    var toString:String = "{"
    val keyIte=this.keySet().iterator()
    while(keyIte.hasNext()){
      val key=keyIte.next()
      toString+="\""+key+"\":"+this.get(key)+","
    }
    if(toString.equals("{"))
      toString="{}"
    else
      toString=toString.substring(0, toString.length()-1)+"}"

    toString
  }

  def copyFromLinkedHashMap(linkedHashMap:util.LinkedHashMap[K,V]):CustomizedHashMap[K,V]={
    this.putAll(linkedHashMap)
    this
  }
}
