package com.yling.assciationrule

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 2018/3/28.
 */
object FP_Growth {

  def main(args:Array[String]){

    if(args.length!=4){
      println("请输入4个参数 购物篮数据路径 最小支持度 最小置信度 数据分区")
      System.exit(0)
    }

    val conf = new SparkConf().setAppName("FPGrowthTest")
    val sc = new SparkContext(conf)

    val data_path=args(0)
    //设置参数
    //最小支持度
    val minSupport=args(1).toDouble
    //最小置信度
    val minConfidence=args(2).toDouble
    //数据分区
    val numPartitions=args(3).toInt


    //取出数据
    val data = sc.textFile(data_path)
    //把数据通过空格分割
    val transactions=data.map(x=>x.split(" "))
    transactions.cache()

    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    model.freqItemsets.collect().foreach(itemset=>{
      println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })

    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //这里可以把规则写入到Mysql数据库中，以后使用来做推荐
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      println(rule.antecedent.mkString(",")+"-->"+
        rule.consequent.mkString(",")+"-->"+ rule.confidence)
    })
    //查看规则生成的数量
    println(model.generateAssociationRules(minConfidence).collect().length)

    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

  }


}
