package com.yling.recommend.util

import java.io.{FileWriter, PrintWriter, File}
import java.text.SimpleDateFormat
import java.util

import org.ansj.app.keyword.Keyword
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.math._
import java.sql.{DriverManager, PreparedStatement, Connection}

/**
 * Created by zhouzhou on 2018/5/27.
 */



case class TalentDemandRating(userTalentDemandID: String, desTalentDemandID: String, rating: Double) extends scala.Serializable

object ContentBaseRecommendALS {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("talentRecommend").setMaster("local[3]")
    val sparkContext = new SparkContext(conf)
    val sqlContext =  new SQLContext(sparkContext)

    //将数据库的记录映射成为DataFrame
    val talendDemandTableDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_talent_demand", "user" -> "root", "password" -> "123456")).load().withColumnRenamed("id","demandID")
    //将数据库的地理位置坐标信息映射成为DataFrame
    val talendDemanLocationDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_location_tx_info", "user" -> "root", "password" -> "123456")).load()
    //将数据库的省会编码表映射为dataFrame
    val talendProviceDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_sysconf_province", "user" -> "root", "password" -> "123456")).load()
    //将数据库的城市编码表映射为dataFrame
    val talendCityDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_sysconf_city", "user" -> "root", "password" -> "123456")).load()
    //将省会城市映射的表和人才需求的表join连接
    val talendOneDF = talendDemandTableDF.join(talendProviceDF,talendDemandTableDF("province_code") === talendProviceDF("province_code"),"left")
    //将城市映射的表和人成才的需求表join连接
    val talendTwoDf = talendOneDF.join(talendCityDF,talendOneDF("city_code")===talendCityDF("city_code"),"left")
    //将地理位置信息表和人才的需求表做join连接
    val talendAllDF = talendTwoDf.join(talendDemanLocationDF,talendTwoDf("province_name")===talendDemanLocationDF("province")&&talendTwoDf("city_name")===talendDemanLocationDF("city"),"left").cache()

    val talentList = talendDemandTableDF.collectAsList()
    //对所有的人才需求的记录进行推荐并且入库
    for(i <- 0 until talentList.size()){
      recommendTalentDemands(sparkContext,sqlContext,talendAllDF,talentList.get(i).get(0).toString)
    }

//    recommendTalentDemands(sparkContext,sqlContext,talendAllDF,"f75f87da-e083-11e7-9523-f1fe683326e2")
    //最后将缓存的数据释放
    talendAllDF.unpersist()


  }

  /**
   * 基于内容推荐的具体实现
   * @param sparkContext
   * @param sqlContext
   * @param userDemandsID
   */
  def recommendTalentDemands(sparkContext:SparkContext,sqlContext:SQLContext,talendAllDF:DataFrame,userDemandsID:String):Unit={
    //初始化所有人才的list的集合
    val talendDemandList= new util.ArrayList[TalentDemand]()
    val srcTalentMap = new util.HashMap[String,TalentDemand]()
    val talentList = talendAllDF.collectAsList()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //获得数据库中点击的需求详情的记录
    val userTalentDemandrow =  talendAllDF.filter(talendAllDF("demandID")===userDemandsID).first()
    var userStartTime:util.Date = null
    var userEndTime:util.Date = null
    var userlocationLat:Double =0
    var userlocationLan:Double =0
    var startTime:util.Date = null
    var endTime:util.Date = null
    var locationLat:Double =0
    var locationLan:Double =0
    if(null == userTalentDemandrow.get(12) || null == userTalentDemandrow.get(9)){
      userStartTime = dateFormat.parse("1988-01-01 00:00:00")
      userEndTime = dateFormat.parse("1988-01-01 00:00:00")
    }else{
      userStartTime = dateFormat.parse(userTalentDemandrow.get(12).toString)
      userEndTime = dateFormat.parse(userTalentDemandrow.get(9).toString)
    }

    val userTalentDemand = new TalentDemand(userTalentDemandrow.get(0).toString,userTalentDemandrow.get(8).toString,userStartTime,userEndTime,112.3345,156.12334)
    for(i <- 0 until talentList.size()){
      //对原始数据中时间进行NULL值处理
      if(null == talentList.get(i).get(12)){
        startTime =dateFormat.parse("1988-01-01 00:00:00")
      }else{
        startTime = dateFormat.parse(talentList.get(i).get(12).toString)
      }
      if(null == talentList.get(i).get(9)){
        endTime = dateFormat.parse("1988-01-01 00:00:00")
      }else{
        endTime = dateFormat.parse(talentList.get(i).get(9).toString)
      }
      //对原始数据中地理位置坐标进行NULL值处理
      if(null == talentList.get(i).getAs[Double]("lat")){
        locationLat = 0
      }else{
        locationLat = talentList.get(i).getAs[Double]("lat")
      }
      if(Nil == talentList.get(i).getAs[Double]("lng")){
        locationLan = 0
      }else{
        locationLan = talentList.get(i).getAs[Double]("lng")
      }
      val srcTalentDemand = new TalentDemand(talentList.get(i).get(0).toString,talentList.get(i).get(8).toString,startTime,endTime,locationLan,locationLat)
      talendDemandList.add(srcTalentDemand)
      srcTalentMap.put(talentList.get(i).get(0).toString,srcTalentDemand)
    }
    //所有人才需求提供的任务内容的关键词列表的Map
    val demandsKeyWordsMap= new util.HashMap[String, util.List[Keyword]]
    //点击人才需求的详情的关键词集合Map
    val userDemandsKeywordsMap = userDemandsKeywordsListMap(userTalentDemand)
    import scala.collection.JavaConversions._
    for(talentDemand <- talendDemandList){
      demandsKeyWordsMap.put(talentDemand.TalentDemandID,TFIDFUtil.getTFIDE(talentDemand.content,10))
    }
    if (null != userTalentDemand) {
      var tempMatchMap = new util.HashMap[String,Double]()
      val allTalentDemandite = demandsKeyWordsMap.keySet().iterator()
      while(allTalentDemandite.hasNext) {
      val talentDemandId = allTalentDemandite.next()
      if (null != userDemandsKeywordsMap.get(userTalentDemand.TalentDemandID)) {
        tempMatchMap.put(talentDemandId, getMatchValue(userDemandsKeywordsMap.get(userTalentDemand.TalentDemandID), demandsKeyWordsMap.get(talentDemandId)))
      }
    }
    //计算地理位置和时间属性的权重分数值
    tempMatchMap = getAllMatchScoreWithSupply(tempMatchMap,userTalentDemand,srcTalentMap)
    removeZeroItem(tempMatchMap)
    //将用户的人才需求ID，人才的供求ID，评分值的形式写入文件，供ALS模型训练进行推荐
    saveUserTalendDemandDataToFile(userDemandsID,tempMatchMap)

//    if(!(tempMatchMap.toString.equals("{}"))){
//      val sorttempMatchMap = sortMapByValue(tempMatchMap)
//      var toberecomend = sorttempMatchMap.keySet()
//      if(toberecomend.size()> 10){
//          val truerecommend = removeOverNews(toberecomend,10)
//          val recommendRDD = sparkContext.parallelize(truerecommend.toSeq)
//          recommendRDD.foreachPartition(partition =>{
//            val conn = DriverManager.getConnection("jdbc:mysql://10.176.4.171:3306/agsrv_db_1031","root", "yly@123")
//            val sql = "insert into bd_recomend_talent(talentDemandID, recommendDemandID) values (?, ?)"
//            val ps = conn.prepareStatement(sql)
//            partition.foreach(row=>{
//              ps.setString(1,userDemandsID)
//              ps.setString(2, row)
//              ps.executeUpdate()
//            }
//            )
//            ps.close()
//            conn.close()
//          }
//          )
//      }else{
//          val recommendRDD = sparkContext.parallelize(toberecomend.toSeq)
//          recommendRDD.foreach{row =>
//            saveRecommendResultToMysql(row,userDemandsID)
//        }
//          println("has be least 10"+toberecomend)
//      }
//     }
    }
  }

  /**
   * 移除匹配值为零的值的记录
   * @param map
   */
  def removeZeroItem(map:util.HashMap[String,Double]):Unit={
    val toBeDeleteItemSet = new util.HashSet[String]()
    val ite = map.keySet().iterator()
    while(ite.hasNext){
      val talentDemandsId = ite.next()
      if(map.get(talentDemandsId) <=0){
        toBeDeleteItemSet.add(talentDemandsId)
      }
    }
    val removeIter = toBeDeleteItemSet.iterator()
    while(removeIter.hasNext){
      val item = removeIter.next()
      map.remove(item)
    }
  }


  /**
   * 点击查看的人才需求的关键词与其余需求的关键词匹配程度
   * @param map
   * @param list
   * @return
   */
  def getMatchValue(map:CustomizedHashMap[String, Double],list:util.List[Keyword]):Double={
     val keywordsSet = map.keySet()
     var totalSum:Double = 0
     var totalKeySum:Double = 0
     var totalValueSum:Double = 0
     var matchValue:Double = 0
     val userKeywordFreq = new util.HashMap[Double,Double]()
    import scala.collection.JavaConversions._
     for(keyword <- list){
       if(keywordsSet.contains(keyword.getName)){
         userKeywordFreq.put(map.get(keyword.getName),keyword.getFreq)
       }
     }
    //计算关键词的权重值
    if(userKeywordFreq.size()!=0){
       val ite = userKeywordFreq.entrySet().iterator()
       while(ite.hasNext){
         val userkeword = ite.next()
         totalSum = totalSum + (userkeword.getKey*userkeword.getValue)
         totalKeySum = totalKeySum + math.pow(userkeword.getKey,2)
         totalValueSum = totalValueSum + math.pow(userkeword.getValue,2)
       }
      matchValue = totalSum / (math.sqrt(totalKeySum)+math.sqrt(totalValueSum))
    }

    matchValue
  }

  /**
   * 使用 Map按value进行排序
   * @param oriMap
   * @return
   */
  def sortMapByValue(oriMap:util.HashMap[String,Double]):util.HashMap[String,Double]={
    if(Nil == oriMap ||oriMap.isEmpty){
       Nil
    }
    val sortedMap = new util.LinkedHashMap[String,Double]()
    val entryList = new util.ArrayList[util.Map.Entry[String,Double]](oriMap.entrySet())
    val sortComparator = new MapValueComparator
    util.Collections.sort(entryList,sortComparator)
    val iter = entryList.iterator()
    while(iter.hasNext){
       val tmpEntry = iter.next()
       sortedMap.put(tmpEntry.getKey,tmpEntry.getValue)
    }
    sortedMap
  }

  /**
   * 获得用户的关键词集合和权重
   * @param userTalentDemand
   */
  def userDemandsKeywordsListMap(userTalentDemand:TalentDemand):util.HashMap[String, CustomizedHashMap[String, Double]]={
    val userDemandsKeywordsMap = new util.HashMap[String, CustomizedHashMap[String, Double]]()
    val keywordsMap = new CustomizedHashMap[String,Double]()
    val userKeywordList = TFIDFUtil.getTFIDE(userTalentDemand.content, 10)
    import scala.collection.JavaConversions._
    for(usekeyword  <- userKeywordList){
      keywordsMap.put(usekeyword.getName,usekeyword.getFreq)
    }
    userDemandsKeywordsMap.put(userTalentDemand.TalentDemandID,keywordsMap)
    userDemandsKeywordsMap
  }

  /**
   * 去除数量上超过为算法设置的推荐结果上限值的推荐结果
   * @param set
   * @param N
   */
  def removeOverNews(set:util.Set[String],N:Int):util.Set[String]={
    var i=0
    val ite = set.iterator
    while (ite.hasNext){
      if (i >= N) {
        ite.remove()
        ite.next()
      }else {
        ite.next()
      }
      i = i+1
    }
    set
  }

  /**
   * 计算时间范围和地理位置的权重分数值
   */
 def getAllMatchScoreWithSupply(tempMatchMap:util.HashMap[String,Double],userTalentDemand:TalentDemand,srcTalentDemandMap:util.HashMap[String,TalentDemand]):util.HashMap[String,Double]={
     if(!tempMatchMap.isEmpty){
         var percent:Double = 0
         var count = 0
         val talentDemansIte = tempMatchMap.keySet.iterator
         while(talentDemansIte.hasNext){
           count = count + 1
           val talentDemandsId = talentDemansIte.next()
           val score = tempMatchMap.get(talentDemandsId)
           val srcTalentDemand = srcTalentDemandMap.get(talentDemandsId)
           var distance:Double =0
           //计算时间的重合度的百分比
           if(null!=srcTalentDemand) {
             percent = TimeIntervalUtil.getPercentOfTime(userTalentDemand.startTime, userTalentDemand.endTime, srcTalentDemand.startTime, srcTalentDemand.endTime)
             if (srcTalentDemand.locationLan == 0 || srcTalentDemand.LocationLat == 0) {
               distance = 0
             } else {
               distance = DistanceUtil.getTwoLocationDistance(userTalentDemand.LocationLat, userTalentDemand.locationLan, srcTalentDemand.LocationLat, srcTalentDemand.locationLan)
             }
           }
           val tatalscore = score * 0.5 + percent * 0.2 + distance * 0.3
           println("关键词分数"+score+"====地理位置分数"+distance+"=====时间分数"+percent)
           tempMatchMap.put(talentDemandsId,tatalscore)
         }
     }
    tempMatchMap
 }

  /**
   * 将推荐结果保存到mysql
   * @param recommendDemandID
   * @param userTalentDemandID
   */
  def saveRecommendResultToMysql(recommendDemandID:String,userTalentDemandID:String): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into bd_recomend_talent(talentDemandID, recommendDemandID) values (?, ?)"
    try {
        conn = DriverManager.getConnection("jdbc:mysql://10.176.4.171:3306/agsrv_db_1031","root", "yly@123")
        ps = conn.prepareStatement(sql)
        ps.setString(1,userTalentDemandID)
        ps.setString(2, recommendDemandID)
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

  /**
   * 建立推荐的ALS模型，实现机器学习的模型训练
   * @param sc
   * @param rawUserTalendDemandData
   */
  def alsModel(sc: SparkContext,
            rawUserTalendDemandData: RDD[String]): Unit = {

    val data = buildRatings(rawUserTalendDemandData)

    //将userTalentID变换为int类型
    val userTalentToInt: RDD[(String, Long)] =
      data.map(_.userTalentDemandID).distinct().zipWithUniqueId()
    val reverseUserTalentIDMapping: RDD[(Long, String)] =
      userTalentToInt map { case (l, r) => (r, l)}
    val userTalentIDMap: Map[String, Int] =
      userTalentToInt.collectAsMap().map { case (n, l) => (n, l.toInt)}
    val bUserTalentIDMap = sc.broadcast(userTalentIDMap)


    //将desTalentID变换为int类型
    val desTalentToInt: RDD[(String, Long)] =
      data.map(_.desTalentDemandID).distinct().zipWithUniqueId()
    val reversedesTalentIDMapping: RDD[(Long, String)] =
      desTalentToInt map { case (l, r) => (r, l)}
    val desTalentIDMap: Map[String, Int] =
      desTalentToInt.collectAsMap().map { case (n, l) => (n, l.toInt)}
    val bdesTalentIDMap = sc.broadcast(desTalentIDMap)

    val ratings: RDD[Rating] = data.map { r =>
      Rating(bUserTalentIDMap.value.get(r.userTalentDemandID).get, bdesTalentIDMap.value.get(r.desTalentDemandID).get, r.rating)
    }.cache()
    //使用协同过滤算法建模
    //val model = ALS.trainImplicit(ratings, 10, 10, 0.01, 1.0)
    val model = ALS.train(ratings, 50, 10, 0.0001)
    ratings.unpersist()
    println("打印第一个userFeature")
    println(model.userFeatures.mapValues(_.mkString(", ")).first())

  }

  /**
   * 将用户人才需求的评分值进行数据的处理
   * @param rawUserTalendDemandData
   * @return
   */
  def buildRatings(rawUserTalendDemandData: RDD[String]): RDD[TalentDemandRating] = {
    rawUserTalendDemandData.map { line =>
      val Array(userTalentDemandID, desTalentDemandID, countStr) = line.split(',').map(_.trim)
      var count = countStr.toInt
      count = if (count == -1) 3 else count
      TalentDemandRating(userTalentDemandID, desTalentDemandID, count)
    }
  }

  /**
   * 将人才供需的评分写入到文件
   * @param userTalend
   * @param tempMatchMap
   */
  def saveUserTalendDemandDataToFile(userTalend:String,tempMatchMap:util.HashMap[String,Double]):Unit ={
    val writer = new FileWriter(new File("test.csv" ),true)
    val ite = tempMatchMap.entrySet().iterator()
    while(ite.hasNext){
      val desTalend = ite.next()
      var linecontent=new StringBuilder()
      linecontent.append(userTalend).append(",").append(desTalend.getKey).append(",").append(desTalend.getValue).append("\n")
      writer.write(linecontent.toString())
    }
    writer.close()
  }
}
