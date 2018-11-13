package com.yaling.classification

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by administrator on 2018/8/21.
 */
object DecisionTreeClassification {

  case class RawDataRecord(category: String, text: String)

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("decisiontree").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var srcRDD = sc.textFile("file:///D:/bayes/yling").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }

    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category",$"text",$"words").take(1)

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.select($"category", $"words", $"rawFeatures").take(1)


    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    println(rescaledData.select($"category", $"features").take(1).toString)

    //转换成decisiontree的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainDataRdd, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    // Evaluate model on test instances and compute test error
    val labelAndPreds = testDataRdd.map {p => (model.predict(p.features), p.label)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testDataRdd.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    // Save and load model
    //  model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
    //  val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
  }

}
