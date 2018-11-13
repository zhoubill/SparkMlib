package com.yaling.classification

/**
 * Created by zhouzhou on 2018/8/16.
 */

import java.sql.{DriverManager, PreparedStatement, Connection}

import org.apache.spark.sql.hive.HiveContext

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{SQLContext, Row}


object BayesClasstification {

  case class RawDataRecord(category: String, text: String)

  case class BayesResult(articleid:String,typecode:String,lables:Double)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("article-bayes").setMaster("local[3]")
    val sparkContext = new SparkContext(conf)
    val sqlContext =  new SQLContext(sparkContext)

    val articleTableDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://10.176.4.166:3306/agro_base", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "education_tech_article_bak", "user" -> "root", "password" -> "yly@2016")).load()

    //70%作为训练数据，30%作为测试数据
    val splits = articleTableDF.randomSplit(Array(0.6, 0.4))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()


    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)


    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.select("type_code", "words", "rawFeatures").take(1)


    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select("type_code","features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select("id","type_code","features").map {
      case Row(id:String, label: String, features: Vector) =>
        val truelables = model.predict(Vectors.dense(features.toArray))
        val articleID = id
        val  typecode = label
        BayesResult(articleID,typecode,truelables)
    }

    testDataRdd.foreachPartition(partition =>{
      val conn = DriverManager.getConnection("jdbc:mysql://10.176.12.41:3306/bigdata","root", "123456")
      val sql = "insert into article_bayes (articleID, typecode,truelables) values (?, ?,?)"
      val ps = conn.prepareStatement(sql)
          partition.foreach(row=>{
            ps.setString(1,row.articleid)
            ps.setString(2, row.typecode)
            ps.setDouble(3, row.lables)
            ps.executeUpdate()
          }
          )
          ps.close()
          conn.close()
    })

    sparkContext.stop()

  }


  /**
   * 将分类结果放到mysql数据库
   * @param articleID
   * @param typecode
   */
  def saveRecommendResultToMysql(articleID:String,typecode:String,truelables:Double): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into article_bayes (articleID, typecode,truelables) values (?, ?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://10.176.12.41:3306/bigdata","root", "123456")
      ps = conn.prepareStatement(sql)
      ps.setString(1,articleID)
      ps.setString(2, typecode)
      ps.setDouble(3, truelables)
      ps.executeUpdate()

    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}
