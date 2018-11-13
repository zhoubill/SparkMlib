package com.yling.kmeans

import breeze.linalg.{DenseVector, sum}
import breeze.numerics.pow
import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, Row}
/**
 * Created by zhouzhou on 2018/9/3.
 * 训练模型并且保存模型
 */
object KMeansDemo {

  case class TrainDataRecord(busID: String, features: Vector)

  case class centrePointRecord(classification: Int, features: Vector)

  case class RawDataRecord(busID: String,createat:String,sort:Integer,status:Integer,updateat:String,version:Integer,code:String,lat:Double,lon:Double)

  case  class BusLocationRecord(busID: String,createat:String,sort:Integer,status:Integer,updateat:String,version:Integer,code:String,lat:Double,lon:Double,classification:Integer)

  def main(args: Array[String]) {
    val basepath = if (args.length > 0) args(0) else "/opt/douban/"
    val conf = new SparkConf().setAppName("bus-kmeans")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    //设置hive的参数，实现小文件的合并
    sqlContext.sql("set hive.merge.mapfiles=true")
    sqlContext.sql("set mapred.max.split.size=256000000")
    sqlContext.sql("set mapred.min.split.size.per.node=192000000")
    sqlContext.sql("set mapred.min.split.size.per.rack=192000000")
    sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    sqlContext.sql("use yling_prd")
    val data = sqlContext.sql(
      """
        | select * from ecard_bus_location_info
      """.stripMargin)

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

    //形成k-means需要的训练数据集格式
    val resultRecordRDD = filterRecordRDD.map{x=>
      val features = Vectors.dense(x.lat, x.lon)
      TrainDataRecord(x.busID,features)
    }

    //获取训练集中的坐标向量
    val itemVectors=resultRecordRDD.map(x =>x.features)

    //根据训练集，训练模型
    val itemClusterModel=KMeans.train(itemVectors,28,1000)

    val item_predict=itemClusterModel.predict(itemVectors)

    def computeDistance(v1:DenseVector[Double],v2:DenseVector[Double])=sum(pow(v1-v2,2))

    //      item_predict.map(x =>(x,1)).reduceByKey(_+_).collect().foreach(println(_))

    //保存k-means的模型到hdfs的路径下
    itemClusterModel.save(sc,basepath+"kmeansModel")

    /**
     * 打印出设置的k个点的中心点位置信息
     */
    println("Cluster Centers Information Overview:")
    val centrePointRdd = itemClusterModel.clusterCenters.map{x=>
      val classification = itemClusterModel.predict(x)
      centrePointRecord(classification,x)
    }
    val centrepointRdd = sc.parallelize(centrePointRdd,1)
    centrepointRdd.saveAsTextFile(basepath+"kmeansModel-point/centrepoint")
    sc.stop
  }
}
