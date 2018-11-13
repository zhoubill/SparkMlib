package com.yling.recommend.util

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util

import org.ansj.app.keyword.Keyword
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map


object TalentSupplyRec {

  case class ResultRecord(id: String, score: Double)

  case class TalentDemand(TalentDemandID: String, content: String,startTime:util.Date,endTime:util.Date,locationLan:Double,LocationLat:Double,words:String) extends scala.Serializable

  case class TalentSupply(TalentSupplyID: String, content: String,startTime:util.Date,endTime:util.Date,locationLan:Double,LocationLat:Double,words:String) extends scala.Serializable

  def main (args: Array[String]) {
    val reocommendtype = if (args.length > 0) args(0) else "1"
//    val conf = new SparkConf().setAppName("talent-supply-Recommend").setMaster("local[3]")
//    到时候改为参数传入
//    val reocommendtype = "1"
    val conf = new SparkConf().setAppName("talent-supply-Recommend")
    val sparkContext = new SparkContext(conf)
    val sqlContext =  new SQLContext(sparkContext)


    /**
     * 这一块DataFrame之间的join的连接会触发spark的shuffle，很影响效率
     * 可以将这一部分数据处理的工作放到Hive做成一张表，这样避免了spark的shuffle阶段时间的消耗（优化点）
     */
    //将数据库的记录映射成为DataFrame
    val talentTableDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://10.176.4.70:3306/agsrv_db", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_talent_demand", "user" -> "root", "password" -> "yly@2016")).load().withColumnRenamed("id","demandID").filter("endTime is not null")
    val supplyTableDF =sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://10.176.4.70:3306/agsrv_db", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "bd_talent_supply", "user" -> "root", "password" -> "yly@2016")).load().withColumnRenamed("id","supplyID").filter("endTime is not null")

    val talentList = talentTableDF.collectAsList()
    val supplyList = supplyTableDF.collectAsList()

    //人才需求推荐人才供给
    if(reocommendtype.equalsIgnoreCase("1")){
      removeBatchRecommendRecord()
      for(i <- 0 until talentList.size()){
        recommendTalentDemands(sparkContext,sqlContext,talentTableDF,supplyTableDF,talentList.get(i).get(0).toString,reocommendtype)
      }
    }
    //人才需求推荐人才需求
    else if(reocommendtype.equalsIgnoreCase("2")){
      for(i <- 0 until talentList.size()){
        recommendTalentDemands(sparkContext,sqlContext,talentTableDF,talentTableDF,talentList.get(i).get(0).toString,reocommendtype)
      }
    }
    //人才供给推荐人才需求
    else if(reocommendtype.equalsIgnoreCase("3")){
      for(i <- 0 until supplyList.size()){
        recommendTalentDemands(sparkContext,sqlContext,supplyTableDF,talentTableDF,supplyList.get(i).get(0).toString,reocommendtype)
      }
    }
    //人才供给推荐供给
    else{
      for(i <- 0 until supplyList.size()){
        recommendTalentDemands(sparkContext,sqlContext,supplyTableDF,supplyTableDF,supplyList.get(i).get(0).toString,reocommendtype)
      }
      removeRecommendTmpRecord()
    }
    talentTableDF.unpersist()
    supplyTableDF.unpersist()
    sparkContext.stop()
  }

  /**
   * 基于内容推荐的具体实现
   * @param sparkContext
   * @param sqlContext
   * @param userDemandsID
   */
  def recommendTalentDemands(sparkContext:SparkContext,sqlContext:SQLContext,talendAllDF:DataFrame,supplyAllDF:DataFrame,userDemandsID:String,recommendtype:String):Unit= {
    //初始化所有人才的list的集合
    val talendSupplyList = new util.ArrayList[TalentSupply]()
    val srcTalentMap = new util.HashMap[String, TalentSupply]()
    val supplyList = supplyAllDF.collectAsList()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var userTalentDemandrow:Row = null
    //获得数据库中点击的需求详情的记录
    if (recommendtype.equalsIgnoreCase("1") || recommendtype.equalsIgnoreCase("2")) {
         userTalentDemandrow = talendAllDF.filter(talendAllDF("demandID") === userDemandsID).first()
    }else{
         userTalentDemandrow = talendAllDF.filter(talendAllDF("supplyID") === userDemandsID).first()
    }
    var userStartTime:util.Date = null
    var userEndTime:util.Date = null
    var userlocationLat:Double =0
    var userlocationLan:Double =0
    var startTime:util.Date = null
    var endTime:util.Date = null
    var locationLat:Double =0
    var locationLan:Double =0
    var wordsString:String = null
    var userWordsString:String = null
    if(null == userTalentDemandrow.get(12) || null == userTalentDemandrow.get(9)){
      userStartTime = dateFormat.parse("1988-01-01 00:00:00")
      userEndTime = dateFormat.parse("1988-01-01 00:00:00")
    }else{
      userStartTime = dateFormat.parse(userTalentDemandrow.get(12).toString)
      userEndTime = dateFormat.parse(userTalentDemandrow.get(9).toString)
    }
    if(null == userTalentDemandrow.getAs[Double]("lat")){
      userlocationLat = 0
    }else{
      userlocationLat = userTalentDemandrow.getAs[Double]("lat")
    }
    if(Nil == userTalentDemandrow.getAs[Double]("lng")){
      userlocationLan = 0
    }else{
      userlocationLan = userTalentDemandrow.getAs[Double]("lng")
    }

    userWordsString = userTalentDemandrow.getAs[String]("words")

    val userTalentDemand = new TalentDemand(userTalentDemandrow.get(0).toString,userTalentDemandrow.get(8).toString,userStartTime,userEndTime,userlocationLan,userlocationLat,userWordsString)
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
      //对原始数据中关键词进行处理
      wordsString = supplyList.get(i).getAs[String]("words")

      val srcTalentSupply = new TalentSupply(supplyList.get(i).get(0).toString,supplyList.get(i).get(8).toString,startTime,endTime,locationLan,locationLat,wordsString)
      talendSupplyList.add(srcTalentSupply)
      srcTalentMap.put(supplyList.get(i).get(0).toString,srcTalentSupply)
    }
    //所有人才供应提供的任务内容的关键词列表的Map
    val supplysKeyWordsMap= new util.HashMap[String, CustomizedHashMap[String, Double]]
    //点击人才需求的详情的关键词集合Map
    val userDemandsKeywordsMap = userDemandsKeywordsListMap(userTalentDemand)
    import scala.collection.JavaConversions._
    for(talentSupply <- talendSupplyList){
      val srcKeywordsMap = getKeywordsMapFromContent(talentSupply.words)
      supplysKeyWordsMap.put(talentSupply.TalentSupplyID,srcKeywordsMap)
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
          var toberecomedresult = getResultRecord(truerecommend,sorttempMatchMap)
          val recommendRDD = sparkContext.parallelize(toberecomedresult.toSeq)

        /**
         * spark中foreach和foreachPartition在效率上的优化
         * 特别是数据库的连接等操作
         */
          recommendRDD.foreachPartition(partition =>{
            val conn = DriverManager.getConnection("jdbc:mysql://10.176.4.66:3306/bigdata","root", "123456")
            val sql = "insert into recomend_talent_supply(sourceID, recommendID,type,score) values (?, ?,?,?)"
            val ps = conn.prepareStatement(sql)
            partition.foreach(row=>{
              ps.setString(1,userDemandsID)
              ps.setString(2, row.id)
              ps.setString(3, recommendtype)
              ps.setDouble(4,row.score)
              ps.executeUpdate()
            }
            )
            ps.close()
            conn.close()
          }
          )
      }else{
          var toberecomedresult = getResultRecord(toberecomend,sorttempMatchMap)
          val recommendRDD = sparkContext.parallelize(toberecomedresult.toSeq)
          recommendRDD.foreach{row =>
            saveRecommendResultToMysql(row.id,userDemandsID,recommendtype,row.score)
        }
          println("has be least 10"+toberecomend)
      }
     }
    }
  }

  /**
   * 返回带匹配度分数的推荐记录
   * @param set
   * @param sorttempMatchMap
   * @return
   */
  def getResultRecord(set:util.Set[String],sorttempMatchMap:util.HashMap[String,Double]): util.Set[ResultRecord] ={
    val ite = set.iterator
    val toBeRecomendSet = new util.HashSet[ResultRecord]()
    while (ite.hasNext){
      val recommendId = ite.next()
      val score = sorttempMatchMap.get(recommendId)
      val resultrecord = new ResultRecord(recommendId,score)
      toBeRecomendSet.add(resultrecord)
    }
    toBeRecomendSet
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
   * @param srcKeywordMap
   * @return
   */
  def getMatchValue(map:CustomizedHashMap[String, Double],srcKeywordMap:CustomizedHashMap[String, Double]):Double={
     val keywordsSet = map.keySet()
     var totalSum:Double = 0
     var totalKeySum:Double = 0
     var totalValueSum:Double = 0
     var matchValue:Double = 0
     val userKeywordFreq = new util.HashMap[Double,Double]()
     import scala.collection.JavaConversions._
     val srcite = srcKeywordMap.entrySet().iterator()
     while(srcite.hasNext){
       val srckeword = srcite.next()
       if(keywordsSet.contains(srckeword.getKey)){
         userKeywordFreq.put(map.get(srckeword.getKey),srckeword.getValue)
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
    val keywordsMap = getKeywordsMapFromContent(userTalentDemand.words)
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
  def saveRecommendResultToMysql(recommendDemandID:String,userTalentDemandID:String,recommendtype:String,score:Double): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into recomend_talent_supply(sourceID, recommendID,type,score) values (?, ?,?,?)"
    try {
        conn = DriverManager.getConnection("jdbc:mysql://10.176.4.66:3306/bigdata","root", "123456")
        ps = conn.prepareStatement(sql)
        ps.setString(1,userTalentDemandID)
        ps.setString(2, recommendDemandID)
        ps.setString(3, recommendtype)
        ps.setDouble(4,score)
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

  /** *
    * 删除实时推荐的临时表记录
    */
  def removeRecommendTmpRecord(): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "truncate table bd_talent_recomm_tmp "
    try {
      conn = DriverManager.getConnection("jdbc:mysql://10.176.4.70:3306/agsrv_db","root", "yly@2016")
      ps = conn.prepareStatement(sql)
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

  /** *
    * 删除实时推荐的临时表记录
    */
  def removeBatchRecommendRecord(): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "truncate table recomend_talent_supply "
    try {
      conn = DriverManager.getConnection("jdbc:mysql://10.176.4.66:3306/bigdata","root", "123456")
      ps = conn.prepareStatement(sql)
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
   * 根据关键词字符串获取关键词分数
   * @param wordsString
   * @return
   */
  def getKeywordsMapFromContent(wordsString:String):CustomizedHashMap[String,Double]={
    val keywordMap = new CustomizedHashMap[String,Double]()
    if(null != wordsString){
      val wordList = wordsString.split(",")
      if(!wordList.isEmpty) {
        for (usekeyword <- wordList) {
          val keywordTerm = usekeyword.split(":")
          println("获取关键词"+keywordTerm(0)+":"+keywordTerm(1))
          keywordMap.put(keywordTerm(0), keywordTerm(1).toDouble)
        }
      }
    }
    keywordMap
  }
}
