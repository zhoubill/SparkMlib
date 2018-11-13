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
 * 人才需求和人才供应撮合关联的功能实现
 * Created by zhouzhou on 2018/5/27.
 */

case class TalentDemand(TalentDemandID: String, content: String,startTime:util.Date,endTime:util.Date,locationLan:Double,LocationLat:Double) extends scala.Serializable

case class TalentSupply(TalentSupplyID: String, content: String,startTime:util.Date,endTime:util.Date,locationLan:Double,LocationLat:Double) extends scala.Serializable


object ContentBaseRecommend {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("talentRecommend").setMaster("local[3]")
    val sparkContext = new SparkContext(conf)
    val sqlContext =  new SQLContext(sparkContext)

    /**
     * 这一块DataFrame之间的join的连接会触发spark的shuffle，很影响效率
     * 可以将这一部分数据处理的工作放到Hive做成一张表，这样避免了spark的shuffle阶段时间的消耗（优化点）
     */
    //将数据库的记录映射成为DataFrame
    val talendDemandTableDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_talent_demand", "user" -> "root", "password" -> "123456")).load().withColumnRenamed("id","demandID")
    val talendsupplyTableDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_talent_supply", "user" -> "root", "password" -> "123456")).load().withColumnRenamed("id","supplyID")
    //将数据库的地理位置坐标信息映射成为DataFrame
    val talendDemanLocationDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_location_tx_info", "user" -> "root", "password" -> "123456")).load()
    //将数据库的省会编码表映射为dataFrame
    val talendProviceDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_sysconf_province", "user" -> "root", "password" -> "123456")).load()
    //将数据库的城市编码表映射为dataFrame
    val talendCityDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3088/agsrv_db_1031", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_sysconf_city", "user" -> "root", "password" -> "123456")).load()
    //将省会城市映射的表和人才需求的表join连接
    val talendOneDF = talendDemandTableDF.join(talendProviceDF,talendDemandTableDF("province_code") === talendProviceDF("province_code"),"left")
    //将省会城市映射的表和人才供应的表join连接
    val supplyOneDF = talendsupplyTableDF.join(talendProviceDF,talendsupplyTableDF("province_code") === talendProviceDF("province_code"),"left")
    //将城市映射的表和人成才的需求表join连接
    val talendTwoDf = talendOneDF.join(talendCityDF,talendOneDF("city_code")===talendCityDF("city_code"),"left")
    //将城市映射的表和人成才的供应表join连接
    val supplyTwoDf = supplyOneDF.join(talendCityDF,supplyOneDF("city_code")===talendCityDF("city_code"),"left")
    //将地理位置信息表和人才的需求表做join连接
    val talendAllDF = talendTwoDf.join(talendDemanLocationDF,talendTwoDf("province_name")===talendDemanLocationDF("province")&&talendTwoDf("city_name")===talendDemanLocationDF("city"),"left").cache()
    //将地理位置信息表和人才的需求表做join连接
    val supplyAllDF = supplyTwoDf.join(talendDemanLocationDF,supplyTwoDf("province_name")===talendDemanLocationDF("province")&&supplyTwoDf("city_name")===talendDemanLocationDF("city"),"left").cache()

    val talentList = talendDemandTableDF.collectAsList()
    //对所有的人才需求的记录进行推荐并且入库
//    for(i <- 0 until talentList.size()){
//      recommendTalentDemands(sparkContext,sqlContext,talendAllDF,supplyAllDF,talentList.get(i).get(0).toString)
//    }
    recommendTalentDemands(sparkContext,sqlContext,talendAllDF,supplyAllDF,"013548ca-363e-4aac-9b18-6f859b4fb81b")
    //最后将缓存的数据释放
    talendAllDF.unpersist()
    supplyAllDF.unpersist()

  }

  /**
   * 基于内容推荐的具体实现
   * @param sparkContext
   * @param sqlContext
   * @param userDemandsID
   */
  def recommendTalentDemands(sparkContext:SparkContext,sqlContext:SQLContext,talendAllDF:DataFrame,supplyAllDF:DataFrame,userDemandsID:String):Unit={
    //初始化所有人才的list的集合
    val talendSupplyList= new util.ArrayList[TalentSupply]()
    val srcTalentMap = new util.HashMap[String,TalentSupply]()
    val supplyList = supplyAllDF.collectAsList()
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
    for(i <- 0 until supplyList.size()){
      //对原始数据中时间进行NULL值处理
      if(null == supplyList.get(i).get(12)){
        startTime =dateFormat.parse("1988-01-01 00:00:00")
      }else{
        startTime = dateFormat.parse(supplyList.get(i).get(12).toString)
      }
      if(null == supplyList.get(i).get(9)){
        endTime = dateFormat.parse("1988-01-01 00:00:00")
      }else{
        endTime = dateFormat.parse(supplyList.get(i).get(9).toString)
      }
      //对原始数据中地理位置坐标进行NULL值处理
      if(null == supplyList.get(i).getAs[Double]("lat")){
        locationLat = 0
      }else{
        locationLat = supplyList.get(i).getAs[Double]("lat")
      }
      if(Nil == supplyList.get(i).getAs[Double]("lng")){
        locationLan = 0
      }else{
        locationLan = supplyList.get(i).getAs[Double]("lng")
      }
      val srcTalentSupply = new TalentSupply(supplyList.get(i).get(0).toString,supplyList.get(i).get(8).toString,startTime,endTime,locationLan,locationLat)
      talendSupplyList.add(srcTalentSupply)
      srcTalentMap.put(supplyList.get(i).get(0).toString,srcTalentSupply)
    }
    //所有人才供应提供的任务内容的关键词列表的Map
    val supplysKeyWordsMap= new util.HashMap[String, util.List[Keyword]]
    //点击人才需求的详情的关键词集合Map
    val userDemandsKeywordsMap = userDemandsKeywordsListMap(userTalentDemand)
    import scala.collection.JavaConversions._
    for(talentSupply <- talendSupplyList){
      supplysKeyWordsMap.put(talentSupply.TalentSupplyID,TFIDFUtil.getTFIDE(talentSupply.content,10))
    }
    if (null != userTalentDemand) {
      var tempMatchMap = new util.HashMap[String,Double]()
      val allTalentsupplyite = supplysKeyWordsMap.keySet().iterator()
      while(allTalentsupplyite.hasNext) {
      val talentSupplyId = allTalentsupplyite.next()
      if (null != userDemandsKeywordsMap.get(userTalentDemand.TalentDemandID)) {
        tempMatchMap.put(talentSupplyId, getMatchValue(userDemandsKeywordsMap.get(userTalentDemand.TalentDemandID), supplysKeyWordsMap.get(talentSupplyId)))
      }
    }
    //计算地理位置和时间属性的权重分数值
    tempMatchMap = getAllMatchScoreWithSupply(tempMatchMap,userTalentDemand,srcTalentMap)
    removeZeroItem(tempMatchMap)

    if(!(tempMatchMap.toString.equals("{}"))){
      val sorttempMatchMap = sortMapByValue(tempMatchMap)
      var toberecomend = sorttempMatchMap.keySet()
      if(toberecomend.size()> 10){
          val truerecommend = removeOverNews(toberecomend,10)
          val recommendRDD = sparkContext.parallelize(truerecommend.toSeq)

        /**
         * spark中foreach和foreachPartition在效率上的优化
         * 特别是数据库的连接等操作
         */
          recommendRDD.foreachPartition(partition =>{
            val conn = DriverManager.getConnection("jdbc:mysql://localhost:3088/agsrv_db_1031","root", "123456")
            val sql = "insert into bd_recomend_talent(talentDemandID, recommendDemandID) values (?, ?)"
            val ps = conn.prepareStatement(sql)
            partition.foreach(row=>{
              ps.setString(1,userDemandsID)
              ps.setString(2, row)
              ps.executeUpdate()
            }
            )
            ps.close()
            conn.close()
          }
          )
      }else{
          val recommendRDD = sparkContext.parallelize(toberecomend.toSeq)
          recommendRDD.foreach{row =>
            saveRecommendResultToMysql(row,userDemandsID)
        }
          println("has be least 10"+toberecomend)
      }
     }
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
      val talentSupplyId = ite.next()
      if(map.get(talentSupplyId) <=0){
        toBeDeleteItemSet.add(talentSupplyId)
      }
    }
    val removeIter = toBeDeleteItemSet.iterator()
    while(removeIter.hasNext){
      val item = removeIter.next()
      map.remove(item)
    }
  }


  /**
   * 点击查看的人才供应的关键词与其余需求的关键词匹配程度
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
      matchValue = totalSum / (math.sqrt(totalKeySum)*math.sqrt(totalValueSum))
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
 def getAllMatchScoreWithSupply(tempMatchMap:util.HashMap[String,Double],userTalentDemand:TalentDemand,srcTalentSupplyMap:util.HashMap[String,TalentSupply]):util.HashMap[String,Double]={
     if(!tempMatchMap.isEmpty){
         var percent:Double = 0
         var count = 0
         val talentSupplyIte = tempMatchMap.keySet.iterator
         while(talentSupplyIte.hasNext){
           count = count + 1
           val talentSupplyId = talentSupplyIte.next()
           val score = tempMatchMap.get(talentSupplyId)
           val srcTalentSupply = srcTalentSupplyMap.get(talentSupplyId)
           var distance:Double =0
           //计算时间的重合度的百分比
           if(null!=srcTalentSupply) {
             percent = TimeIntervalUtil.getPercentOfTime(userTalentDemand.startTime, userTalentDemand.endTime, srcTalentSupply.startTime, srcTalentSupply.endTime)
             if (srcTalentSupply.locationLan == 0 || srcTalentSupply.LocationLat == 0) {
               distance = 0
             } else {
               distance = DistanceUtil.getTwoLocationDistance(userTalentDemand.LocationLat, userTalentDemand.locationLan, srcTalentSupply.LocationLat, srcTalentSupply.locationLan)
             }
           }
           val tatalscore = score * 0.5 + percent * 0.2 + distance * 0.3
           println("关键词分数"+score+"====地理位置分数"+distance+"=====时间分数"+percent)
           tempMatchMap.put(talentSupplyId,tatalscore)
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
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3088/agsrv_db_1031","root", "123456")
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
}
