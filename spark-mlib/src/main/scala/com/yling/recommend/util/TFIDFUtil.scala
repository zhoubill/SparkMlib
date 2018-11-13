package com.yling.recommend.util

import java.util.List;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;

/**
 * Created by administrator on 2018/5/25.
 */
class TFIDFUtil {

}

object TFIDFUtil{

  def split(text:String):Result={
    ToAnalysis.parse(text)
  }


  def getTFIDE(title:String,content:String,keyNums:Int):List[Keyword]={
    val kwc = new KeyWordComputer(keyNums)
    kwc.computeArticleTfidf(title, content)
  }

  def getTFIDE(content:String,keyNums:Int):List[Keyword]={
    val kwc = new KeyWordComputer(keyNums)
    kwc.computeArticleTfidf(content)
  }
}

object TFIDFTest{
  def main(args: Array[String]): Unit = {
    val content="我今天很开心，所以一口气买了好多东西。然而我一不小心把本月预算透支了，现在有很不开心了，因为后面的日子得吃土了！"
    var data=TFIDFUtil.getTFIDE(content, 6)
    println(data);
  }
}
