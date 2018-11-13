package com.yling.kmeans


import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by administrator on 2018/10/23.
 */
object CentrePointSite {

  case class sitePoint(siteName: String,lat: Double,lon: Double,classification:Int)

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("bus-site").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val kemansModel = KMeansModel.load(sc, "file:///D:/bayes/kmeansModel")
    var validateRDD = sc.textFile("file:///D:/bayes/yling-bus").map {
      lines =>
      val tokens = lines.split(",")
      val lat =  tokens(1).toDouble
      val lon =  tokens(2).toDouble
      val features = Vectors.dense(lat, lon)
      val prediction = kemansModel.predict(features)
        sitePoint(tokens(0),lat,lon,prediction)
    }

    validateRDD.foreach{ x=>println("站点位置信息"+x.siteName+"经纬度："+x.lat+"--"+x.lon+"所属类别："+x.classification)}

  }

}
