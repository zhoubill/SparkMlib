package com.yaling.classification

import java.sql.DriverManager

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by administrator on 2018/10/29.
 */
object ArticleResultIntoMysql {

  case class BayesResult(articleid:String,typecode:String,lables:Double)

  def main(args: Array[String]) {
    val basepath = if (args.length > 0) args(0) else "/opt/douban/"
    val conf = new SparkConf().setAppName("article-result-bayes")
    val sparkContext = new SparkContext(conf)

    val sqlContext = new HiveContext(sparkContext)
    val hiveSql = " select * from education_tech_article "
    //设置hive的参数，实现小文件的合并
    sqlContext.sql("set hive.merge.mapfiles=true")
    sqlContext.sql("set mapred.max.split.size=256000000")
    sqlContext.sql("set mapred.min.split.size.per.node=192000000")
    sqlContext.sql("set mapred.min.split.size.per.rack=192000000")
    sqlContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
    sqlContext.sql("use yling_prd")
    val data = sqlContext.sql(hiveSql.stripMargin)


    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    var wordsData = tokenizer.transform(data)

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

    val model = NaiveBayesModel.load(sparkContext, basepath+"BayesModel")

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select("id","type_code","features").map {
      case Row(id:String, label: String, features: Vector) =>
        val truelables = model.predict(Vectors.dense(features.toArray))
        val articleID = id
        val  typecode = label
        BayesResult(articleID,typecode,truelables)
    }

    /**
     * 将最后结果写入hive的结果表
     */
    import sqlContext.implicits._
    val buslocationDF = trainDataRdd.toDF()
    buslocationDF.write.mode(SaveMode.Overwrite).saveAsTable("yling_prd.education_tech_article_result")

//    trainDataRdd.foreachPartition(partition =>{
//      val conn = DriverManager.getConnection("jdbc:mysql://10.176.12.41:3306/bigdata","root", "123456")
//      val sql = "insert into article_bayes (articleID, typecode,truelables) values (?, ?,?)"
//      val ps = conn.prepareStatement(sql)
//      partition.foreach(row=>{
//        ps.setString(1,row.articleId)
//        ps.setString(2, row.typeCode)
//        ps.setDouble(3, row.lables)
//        ps.executeUpdate()
//      }
//      )
//      ps.close()
//      conn.close()
//    })

    sparkContext.stop()
  }

}
