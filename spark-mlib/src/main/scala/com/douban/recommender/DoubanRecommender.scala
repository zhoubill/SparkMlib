/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.douban.recommender

import scala.collection.Map
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.spark.sql.hive.HiveContext

//scala 样例类
case class MovieRating(userID: String, movieID: Int, rating: Double) extends scala.Serializable

object DoubanRecommender {

  def main(args: Array[String]): Unit = {
    //初始化sparkcontext
    val sc = new SparkContext(new SparkConf().setAppName("DoubanRecommender"))
    val base = if (args.length > 0) args(0) else "/opt/douban/"

    //获取RDD
//    val rawUserMoviesData = sc.textFile("/apps/hive/warehouse/recommender.db/usermovies")
//    val rawHotMoviesData = sc.textFile("/apps/hive/warehouse/recommender.db/hostmovies_2")
    //封装了从hbase获取原始数据（用户评论电影的数据和最热电影的数据）
    val rawUserMoviesData = getToHbase(sc,"usermovies","userinfo","movieid","movierank")
    val rawHotMoviesData = getToHbase(sc,"hostmovies","movie","moviescore","moviename")
    //准备数据
    preparation(rawUserMoviesData, rawHotMoviesData)
    println("准备完数据")

    // model(sc, rawUserMoviesData, rawHotMoviesData)

    //evaluate(sc,rawUserMoviesData, rawHotMoviesData)
    //调用封装的推荐方法
    recommend(sc, rawUserMoviesData, rawHotMoviesData, base)
  }

  //得到电影名字的RDD，RDD里面形式的数据就是（电影ID，电影名字）的map类型的集合
  def buildMovies(rawHotMoviesData: RDD[String]): Map[Int, String] =
    rawHotMoviesData.flatMap { line =>
      val tokens = line.split(',')
      if (tokens(0).isEmpty) {
        None
      } else {
        //some  是scala中取样分析的结果
        Some((tokens(0).toInt, tokens(2)))
      }
    }.collectAsMap()


  //分析清理数据，预处理数据，获取用户ID和电影ID
  def preparation(rawUserMoviesData: RDD[String],
                  rawHotMoviesData: RDD[String]) = {
    val userIDStats = rawUserMoviesData.map(_.split(',')(0).trim).distinct().zipWithUniqueId().map(_._2.toDouble).stats()
    val itemIDStats = rawUserMoviesData.map(_.split(',')(1).trim.toDouble).distinct().stats()
    println(userIDStats)
    println(itemIDStats)

    val moviesAndName = buildMovies(rawHotMoviesData)

    val (movieID, movieName) = moviesAndName.head
    println(movieID + " -> " + movieName)
  }

  //处理用户评分的电影数据，将用户评分的电影数据转换成ALS模型需要的Rating数据形式
  def buildRatings(rawUserMoviesData: RDD[String]): RDD[MovieRating] = {
    rawUserMoviesData.map { line =>
      val Array(userID, moviesID, countStr) = line.split(',').map(_.trim)
      var count = countStr.toInt
      count = if (count == -1) 3 else count
      MovieRating(userID, moviesID.toInt, count)
    }
  }

  //http://stackoverflow.com/questions/27772769/spark-how-to-use-mllib-recommendation-if-the-user-ids-are-string-instead-of-co
  def model(sc: SparkContext,
            rawUserMoviesData: RDD[String],
            rawHotMoviesData: RDD[String]): Unit = {

    val moviesAndName = buildMovies(rawHotMoviesData)
    val bMoviesAndName = sc.broadcast(moviesAndName)

    val data = buildRatings(rawUserMoviesData)

    val userIdToInt: RDD[(String, Long)] =
      data.map(_.userID).distinct().zipWithUniqueId()
    val reverseUserIDMapping: RDD[(Long, String)] =
      userIdToInt map { case (l, r) => (r, l)}

    val userIDMap: Map[String, Int] =
      userIdToInt.collectAsMap().map { case (n, l) => (n, l.toInt)}

    val bUserIDMap = sc.broadcast(userIDMap)

    val ratings: RDD[Rating] = data.map { r =>
      Rating(bUserIDMap.value.get(r.userID).get, r.movieID, r.rating)
    }.cache()
    //使用协同过滤算法建模
    //val model = ALS.trainImplicit(ratings, 10, 10, 0.01, 1.0)
    val model = ALS.train(ratings, 50, 10, 0.0001)
    ratings.unpersist()
    println("打印第一个userFeature")
    println(model.userFeatures.mapValues(_.mkString(", ")).first())

    for (userID <- Array(100, 1001, 10001, 100001, 110000)) {
      checkRecommenderResult(userID, rawUserMoviesData, bMoviesAndName, reverseUserIDMapping, model)
    }

    unpersist(model)
  }


  //查看给某个用户的推荐
  def checkRecommenderResult(userID: Int, rawUserMoviesData: RDD[String], bMoviesAndName: Broadcast[Map[Int, String]], reverseUserIDMapping: RDD[(Long, String)], model: MatrixFactorizationModel): Unit = {

    val userName = reverseUserIDMapping.lookup(userID).head

    val recommendations = model.recommendProducts(userID, 5)
    //给此用户的推荐的电影ID集合
    val recommendedMovieIDs = recommendations.map(_.product).toSet

    //得到用户点播的电影ID集合
    val rawMoviesForUser = rawUserMoviesData.map(_.split(',')).
      filter { case Array(user, _, _) => user.trim == userName}
    val existingUserMovieIDs = rawMoviesForUser.map { case Array(_, movieID, _) => movieID.toInt}.
      collect().toSet


    println("用户" + userName + "点播过的电影名")
    //点播的电影名
    bMoviesAndName.value.filter { case (id, name) => existingUserMovieIDs.contains(id)}.values.foreach(println)

    println("推荐给用户" + userName + "的电影名")
    //推荐的电影名
    bMoviesAndName.value.filter { case (id, name) => recommendedMovieIDs.contains(id)}.values.foreach(println)
  }


  //调整模型算法参数的训练方法，利用方差形式MSE的值来确定参数是否为最优
  def evaluate(sc: SparkContext,
               rawUserMoviesData: RDD[String],
               rawHotMoviesData: RDD[String]): Unit = {
    val moviesAndName = buildMovies(rawHotMoviesData)
    val data = buildRatings(rawUserMoviesData)

    val userIdToInt: RDD[(String, Long)] =
      data.map(_.userID).distinct().zipWithUniqueId()


    val userIDMap: Map[String, Int] =
      userIdToInt.collectAsMap().map { case (n, l) => (n, l.toInt)}

    val bUserIDMap = sc.broadcast(userIDMap)

    val ratings: RDD[Rating] = data.map { r =>
      Rating(bUserIDMap.value.get(r.userID).get, r.movieID, r.rating)
    }.cache()

    val numIterations = 10

    for (rank <- Array(10, 50);
         lambda <- Array(1.0, 0.01, 0.0001)) {
      val model = ALS.train(ratings, rank, numIterations, lambda)

      // Evaluate the model on rating data
      val usersMovies = ratings.map { case Rating(user, movie, rate) =>
        (user, movie)
      }
      val predictions =
        model.predict(usersMovies).map { case Rating(user, movie, rate) =>
          ((user, movie), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, movie, rate) =>
        ((user, movie), rate)
      }.join(predictions)

      val MSE = ratesAndPreds.map { case ((user, movie), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
      println(s"(rank:$rank, lambda: $lambda, Explicit ) Mean Squared Error = " + MSE)
    }

    for (rank <- Array(10, 50);
         lambda <- Array(1.0, 0.01, 0.0001);
         alpha <- Array(1.0, 40.0)) {
      val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

      // Evaluate the model on rating data
      val usersMovies = ratings.map { case Rating(user, movie, rate) =>
        (user, movie)
      }
      val predictions =
        model.predict(usersMovies).map { case Rating(user, movie, rate) =>
          ((user, movie), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, movie, rate) =>
        ((user, movie), rate)
      }.join(predictions)

      val MSE = ratesAndPreds.map { case ((user, movie), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
      println(s"(rank:$rank, lambda: $lambda,alpha:$alpha ,implicit  ) Mean Squared Error = " + MSE)
    }
  }

  def recommend(sc: SparkContext,
                rawUserMoviesData: RDD[String],
                rawHotMoviesData: RDD[String],
                base: String): Unit = {
    val moviesAndName = buildMovies(rawHotMoviesData)
//    spark的广播变量  优化性能  大小表连接  优先将小表采取广播变量
    val bMoviesAndName = sc.broadcast(moviesAndName)
//userID为String类型的MovieRatings的RDD集合
    val data = buildRatings(rawUserMoviesData)

      /** user的ID有些是不是int类型的，需要将其转化成int类型 zipWithUniqueId方法是将RDD中元素和一个唯一ID组合成键/值对，
        *  该唯一ID生成算法如下：
        *   每个分区中第一个元素的唯一ID值为：该分区索引号，
         *  每个分区中第N个元素的唯一ID值为：(前一个元素的唯一ID值) + (该RDD总的分区数)
        **/
    val userIdToInt: RDD[(String, Long)] =
      data.map(_.userID).distinct().zipWithUniqueId()
    val reverseUserIDMapping: RDD[(Long, String)] =
      userIdToInt map { case (l, r) => (r, l)}

    val userIDMap: Map[String, Int] =
      userIdToInt.collectAsMap().map { case (n, l) => (n, l.toInt)}

    val bUserIDMap = sc.broadcast(userIDMap)
    val bReverseUserIDMap = sc.broadcast(reverseUserIDMapping.collectAsMap())

    val ratings: RDD[Rating] = data.map { r =>
      Rating(bUserIDMap.value.get(r.userID).get, r.movieID, r.rating)
    }.cache()
    //使用协同过滤算法建模
    //val model = ALS.trainImplicit(ratings, 10, 10, 0.01, 1.0)
    val model = ALS.train(ratings, 50, 10, 0.0001)
    ratings.unpersist()

     //model.save(sc, base + "model")
     //val sameModel = MatrixFactorizationModel.load(sc, base + "model")

    //此段代码是为每个用户推荐5个电影而形成的RDD集合
    val allRecommendations = model.recommendProductsForUsers(5) map {
      case (userID, recommendations) => {
        var recommendationStr = ""
        for (r <- recommendations) {
          recommendationStr += r.product + ":" + bMoviesAndName.value.getOrElse(r.product, "") + ","
        }
        if (recommendationStr.endsWith(","))
          recommendationStr = recommendationStr.substring(0, recommendationStr.length - 1)

        (bReverseUserIDMap.value.get(userID).get, recommendationStr)
      }
    }


//    allRecommendations.saveAsTextFile(base + "result.csv")
//    allRecommendations.coalesce(1,true).saveAsTextFile("hdfs://bigdata-hdp01.yling.com:8020/opt/douban/result.csv"+System.currentTimeMillis())
    //最后将推荐结果的RDD存储到HBASE中
     putToHbase(sc,allRecommendations.coalesce(1,true))
    //释放占用内存的model
    unpersist(model)
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  def putToHbase(sc: SparkContext,
                 allRecommendations: RDD[(String,String)]):Unit = {
        val conf = HBaseConfiguration.create()
        //设置zooKeeper集群地址
        conf.set("hbase.zookeeper.quorum","bigdata-hdp01.yling.com,bigdata-hdp02.yling.com,bigdata-hdp03.yling.com")
       //设置zookeeper连接端口，默认2181
        conf.set("hbase.zookeeper.property.clientPort", "2181")
       //设置zookeeper的hbase的根目录
        conf.set("zookeeper.znode.parent","/hbase-unsecure")
        val tablename = "recommend"

        //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
        val jobConf = new JobConf(conf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
        val rdd = allRecommendations.map(x => x._1+','+x._2).map(_.split(',')).map{arr=>{
        /*一个Put对象就是一行记录，在构造方法中指定主键
         * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
         * Put.add方法接收三个参数：列族，列名，数据
        */
        val put = new Put(Bytes.toBytes(arr(0)))
          put.add(Bytes.toBytes("recomendmovies"),Bytes.toBytes("movies1"),Bytes.toBytes(arr(1)))
          put.add(Bytes.toBytes("recomendmovies"),Bytes.toBytes("movies2"),Bytes.toBytes(arr(2)))
          put.add(Bytes.toBytes("recomendmovies"),Bytes.toBytes("movies3"),Bytes.toBytes(arr(3)))
          put.add(Bytes.toBytes("recomendmovies"),Bytes.toBytes("movies4"),Bytes.toBytes(arr(4)))
          put.add(Bytes.toBytes("recomendmovies"),Bytes.toBytes("movies5"),Bytes.toBytes(arr(5)))
        //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
          (new ImmutableBytesWritable, put)
    }}
        rdd.saveAsHadoopDataset(jobConf)
  }

  def getToHbase(sc: SparkContext,tablename: String,cloumnQulifier:String,cloumn1:String,cloumn2:String): RDD[(String)] = {
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    conf.set("hbase.zookeeper.quorum","bigdata-hdp01.yling.com,bigdata-hdp02.yling.com,bigdata-hdp03.yling.com")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //设置zookeeper的hbase的根目录
    conf.set("zookeeper.znode.parent","/hbase-unsecure")

    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tablename)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val stringRecordRDD = hBaseRDD.map{arr=>{
      val key = Bytes.toString(arr._2.getRow)
      //通过列族和列名获取列
      val cloumn_1 = Bytes.toString(arr._2.getValue(cloumnQulifier.getBytes,cloumn1.getBytes))
      val cloumn_2 = Bytes.toString(arr._2.getValue(cloumnQulifier.getBytes,cloumn2.getBytes))
      val record = key+','+cloumn_1+','+cloumn_2
      (record)
    }}
    admin.close()
    return stringRecordRDD

  }

  //从hive获取用户行为评分记录
  def getFromHive(sc: SparkContext): RDD[(String)] ={
    val sqlContext = new HiveContext(sc)
    sqlContext.table("recommender.useraction") // 库名.表名 的格式
      .registerTempTable("useraction")  // 注册成临时表
    val data = sqlContext.sql(
      """
        | select *
        |   from useraction
        |  limit 10
      """.stripMargin)
    val stringRecordRDD = data.map{row=>{
      val userID = row.getString(0)
      val movieID = row.getString(1)
      val movieScore = row.getFloat(2)
      val record = userID+','+movieID+','+movieScore
      (record)
     }
    }

    return stringRecordRDD

  }



  }
