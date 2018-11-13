package com.douban.recommender

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2018/3/21.
 */
object SparkHiveTest {



  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HiveTest").set("hive.metastore.uris", "thrift://10.176.4.68:9083")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use recommender")
    val data = sqlContext.sql(
        """
          | select *
          |   from useraction_1
          |  limit 10
        """.stripMargin)
    val stringRecordRDD = data.map { row => {
      val userID = row.getString(0)
      val movieID = row.getString(1)
      val movieScore = row.getString(2)
      val record = userID + ',' + movieID + ',' + movieScore
      (record)
    }
    }
    stringRecordRDD.saveAsTextFile("hdfs://bigdata-hdp01.yling.com:8020/opt/douban/puthive_"+System.currentTimeMillis())
  }
}

