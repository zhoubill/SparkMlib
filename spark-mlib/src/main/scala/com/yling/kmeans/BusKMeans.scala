package com.yling.kmeans

import breeze.linalg.{sum, DenseVector}
import breeze.numerics._
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by zhouzhou on 2018/10/15.
 */
object BusKMeans {
  case class TrainDataRecord(busID: String, features: Vector)

  case class RawDataRecord(busID: String,createat:String,sort:Integer,status:Integer,updateat:String,version:Integer,code:String,lat:Double,lon:Double)

  case  class BusLocationRecord(busID: String,createat:String,sort:Integer,status:Integer,updateat:String,version:Integer,code:String,lat:Double,lon:Double,classification:Integer)

  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("bus-kmeans").set("hive.metastore.uris", "thrift://test-hdp02.yling.com:9083")
      val sc = new SparkContext(conf)
      val sqlContext = new HiveContext(sc)
      sqlContext.sql("use yling_test")
      val data = sqlContext.sql(
        """
         | select *
         |    from ecard_bus_location_info
        """.stripMargin)

      val stringRecordRDD = data.map { row => {
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

//      val srcRDD = sc.textFile("file:///D:/bayes/yling-bus")
//      val stringRecordRDD = srcRDD.map{line =>
//         val tokens = line.split(',')
//         val busID = tokens(0)
//         val createat = tokens(1)
//         val sort = tokens(2).toInt
//         val status = tokens(3).toInt
//         val updateat = tokens(4)
//         val version = tokens(5).toInt
//         val code =  tokens(6)
//         val lat = tokens(7).toDouble
//         val lon = tokens(8).toDouble
//         RawDataRecord(busID,createat,sort,status,updateat,version,code,lat,lon)
//      }

      //处理数据中的异常值
      val filterRecordRDD = stringRecordRDD.filter{x=>x.lon<34.35}

      //形成k-means需要的训练数据集格式
      val resultRecordRDD = filterRecordRDD.map{x=>
          val features = Vectors.dense(x.lat, x.lon)
          TrainDataRecord(x.busID,features)
      }

      //获取训练集中的坐标向量
      val itemVectors=resultRecordRDD.map(x =>x.features)

      //根据训练集，训练模型
      val itemClusterModel=KMeans.train(itemVectors,28,100)

      val item_predict=itemClusterModel.predict(itemVectors)

      def computeDistance(v1:DenseVector[Double],v2:DenseVector[Double])=sum(pow(v1-v2,2))

//      item_predict.map(x =>(x,1)).reduceByKey(_+_).collect().foreach(println(_))

      /**
      * 训练合适的k的值
      */
//    val ks:Array[Int] = Array(3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30)
//    ks.foreach(cluster => {
//      val model:KMeansModel = KMeans.train(itemVectors, cluster,30,1)
//      val ssd = model.computeCost(itemVectors)
//      println("sum of squared distances of points to their nearest center when k=" + cluster + " -> "+ ssd)
//    })

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
    buslocationDF.write.mode(SaveMode.Overwrite).saveAsTable("yling_test.ecard_bus_location_result")

     /**
     * 打印出设置的k个点的中心点位置信息
     */
      println("Cluster Centers Information Overview:")
      var clusterIndex: Int = 0
      itemClusterModel.clusterCenters.foreach(
        x => {
          println("Center Point of Cluster " + clusterIndex + ":")
          println(x)
          clusterIndex += 1
      })

      //使用k-means的模型对数据进行预测
//    srcRDD.map{line =>
//      val tokens = line.split(',')
//      val busID = tokens(0)
//      val lat = tokens(7).toDouble
//      val lon = tokens(8).toDouble
//      val features = Vectors.dense(lat, lon)
//      val prediction = itemClusterModel.predict(features)
//      "bus的定位信息经度："+lat+"bus的定位信息维度"+lon+"bus的id:"+busID+"bus的类别"+prediction
//    }.foreach(println)
    sc.stop
  }
}
