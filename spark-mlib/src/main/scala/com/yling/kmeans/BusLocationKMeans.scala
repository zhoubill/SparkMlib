package com.yling.kmeans

import breeze.linalg.{DenseVector, sum}
import breeze.numerics._
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by zhouzhou on 2018/10/15.
 */
object BusLocationKMeans {
  case class TrainDataRecord(busID: String, features: Vector)

  case class RawDataRecord(busID: String,createat:String,sort:Integer,status:Integer,updateat:String,version:Integer,code:String,lat:Double,lon:Double)

  case  class BusLocationRecord(busID: String,createat:String,sort:Integer,status:Integer,updateat:String,version:Integer,code:String,lat:Double,lon:Double,classification:Integer)

  def main(args: Array[String]) {
      val basepath = if (args.length > 0) args(0) else "/opt/douban/"
      val conf = new SparkConf().setAppName("bus-kmeans")
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)
//      val date = args(1)
      val hiveSql = " select * from ecard_bus_location_info "
      //设置hive的参数，实现小文件的合并
      sqlContext.sql("set hive.merge.mapfiles=true")
      sqlContext.sql("set mapred.max.split.size=256000000")
      sqlContext.sql("set mapred.min.split.size.per.node=192000000")
      sqlContext.sql("set mapred.min.split.size.per.rack=192000000")
      sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      sqlContext.sql("use yling_prd")
      val data = sqlContext.sql(hiveSql.stripMargin)

      val stringRecordRDD = data.map {row => {
        val busID = row.getString(0)
        val createat = row.getString(1)
        val sort = row.getInt(2)
        val status = 0
        val updateat = row.getString(4)
        val version = row.getInt(5)
        val code =  row.getString(6)
        val lat = row.getDouble(7)
        val lon = row.getDouble(8)
        RawDataRecord(busID,createat,sort,status,updateat,version,code,lat,lon)
      }
    }
      //处理数据中的异常值
      val filterRecordRDD = stringRecordRDD.filter{x=>x.lon<34.30}

     val itemClusterModel = KMeansModel.load(sc,basepath+"kmeansModel")

    val resultHiveRdd = stringRecordRDD.map{record =>
        val features = Vectors.dense(record.lat, record.lon)
        val prediction = itemClusterModel.predict(features)
        BusLocationRecord(record.busID,record.createat,record.sort,record.status,record.updateat,record.version,record.code,record.lat,record.lon,prediction)
    }

     /**
     * 将最后结果写入hive的结果表
     */
    import sqlContext.implicits._
    val buslocationDF = resultHiveRdd.toDF()
    buslocationDF.write.mode(SaveMode.Overwrite).saveAsTable("yling_prd.ecard_bus_location_result")

    sc.stop
  }
}
