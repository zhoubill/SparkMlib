package com.yaling.classification

import java.sql.{DriverManager, PreparedStatement, Connection}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, Row}

/**
 * Created by administrator on 2018/10/29.
 */
object ArticleBayesClassification {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]) {
    val basepath = if (args.length > 0) args(0) else "/opt/douban/"
    val conf = new SparkConf().setAppName("article-bayes")
    val sparkContext = new SparkContext(conf)

    val sqlContext = new HiveContext(sparkContext)
    //      val date = args(1)
    val hiveSql = " select * from education_tech_article "
    //设置hive的参数，实现小文件的合并
    sqlContext.sql("set hive.merge.mapfiles=true")
    sqlContext.sql("set mapred.max.split.size=256000000")
    sqlContext.sql("set mapred.min.split.size.per.node=192000000")
    sqlContext.sql("set mapred.min.split.size.per.rack=192000000")
    sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    sqlContext.sql("use yling_prd")
    val data = sqlContext.sql(hiveSql.stripMargin)

    val datarep = data.repartition(6)

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    var wordsData = tokenizer.transform(datarep)

//    println("这个datafram的大小是："+data.collectAsList().size())

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
    println("output4：")
    println(trainDataRdd.take(1).toString)

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    model.save(sparkContext,basepath+"BayesModel")

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
