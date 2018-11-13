package com.yaling.hbase

/**
 * Created by Administrator on 2018/1/25.
 */
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.{Cell, HColumnDescriptor, HTableDescriptor, KeyValue}

import collection.JavaConverters._

object TestHbaeJavaApi {

  private val conf :Configuration = ConfigUtil.apply.createHadoopConfig

  def isExist(tableName: String) {
    val hAdmin: HBaseAdmin = new HBaseAdmin(conf)
    hAdmin.tableExists(tableName)
  }

  def createTable(tableName: String, columnFamilys: Array[String]): Unit = {
    val hAdmin: HBaseAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(tableName)) {
      println("表" + tableName + "已经存在")
      return
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      hAdmin.createTable(tableDesc)
      println("创建表成功")
    }
  }

  def deleteTable(tableName: String): Unit = {
    val admin: HBaseAdmin = new HBaseAdmin(conf)
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      println("删除表成功!")
    } else {
      println("表" + tableName + " 不存在")
    }
  }

  def addRow(tableName: String, row: String, columnFaily: String, column: String, value: String): Unit = {
    val table: HTable = new HTable(conf, tableName)
    val put: Put = new Put(Bytes.toBytes(row))
    put.add(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }

  def delRow(tableName: String, row: String): Unit = {
    val table: HTable = new HTable(conf, tableName)
    val delete: Delete = new Delete(Bytes.toBytes(row))
    table.delete(delete)
  }

  def delMultiRows(tableName: String, rows: Array[String]): Unit = {
    val table: HTable = new HTable(conf, tableName)
    val deleteList = for (row <- rows) yield new Delete(Bytes.toBytes(row))
    table.delete(deleteList.toSeq.asJava)
  }

  def getRow(tableName: String, row: String): Unit = {
    val table: HTable = new HTable(conf, tableName)
    val get: Get = new Get(Bytes.toBytes(row))
    val result: Result = table.get(get)
    for (rowKv <- result.raw()) {
      println(new String(rowKv.getFamily))
      println(new String(rowKv.getQualifier))
      println(rowKv.getTimestamp)
      println(new String(rowKv.getRow))
      println(new String(rowKv.getValue))
    }
  }

  def getAllRows(tableName: String): Unit = {
    val table: HTable = new HTable(conf, tableName)
    val scan: Scan = new Scan()
//    scan.setStartRow(Bytes.toBytes("100000149"))
//    scan.setStopRow(Bytes.toBytes("100008579"))
    val results: ResultScanner = table.getScanner(scan)
    val it: util.Iterator[Result] = results.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      for(kv <- next.raw()){
        println("rowkey:"+Bytes.toInt(kv.getRow)+"cloumnFmaliy:"+new String(kv.getFamily)+"cloumns:"+new String(kv.getQualifier)
          +"value:"+Bytes.toString(kv.getValue)+"timestamp:"+kv.getTimestamp)
        println("---------------------")
      }

      //      val cells: Array[Cell] = next.rawCells()
      //      for (cell <- cells) {
      //        println(new String(cell.getRowArray)+" row")
      //        println(new String(cell.getFamilyArray))
      //        println(new String(cell.getQualifierArray))
      //        println(new String(cell.getValueArray))
      //        println(cell.getTimestamp)
      //        println("---------------------")
      //      }


    }
  }

  def getOneRow(tableName: String, rowkey:String):Unit = {
    val table: HTable = new HTable(conf, tableName)
    val result: Result = table.get(new Get(rowkey.getBytes))
    for (kv <- result.raw()) {
        println("rowkey:" + Bytes.toString(kv.getRow) + "  cloumnFmaliy:" + new String(kv.getFamily) + "  cloumns:" + new String(kv.getQualifier)
          + "  value:" + new String(kv.getValue) + "  timestamp:" + kv.getTimestamp)
        println("---------------------")
      }
    }

  def getOneRealRecommendRow(tableName: String, userID:Int):Unit = {
    val table: HTable = new HTable(conf, tableName)
    val result: Result = table.get(new Get(Bytes.toBytes(userID)))
    for (kv <- result.raw()) {
      println("rowkey:" + Bytes.toInt(kv.getRow) + "  cloumnFmaliy:" + new String(kv.getFamily) + "  cloumns:" + new String(kv.getQualifier)
        + "  value:" + new String(kv.getValue) + "  timestamp:" + kv.getTimestamp)
      println("---------------------")
    }
  }


  def main(args: Array[String]) {
    //TestHbaeJavaApi.createTable("testApi",Array("info","two"))
    //TestHbaeJavaApi.addRow("testApi","row2","info","get","getTwo")
    //TestHbaeJavaApi.delRow("testApi","row2")

    //TestHbaeJavaApi.getRow("testApi","row1")
//      this.getOneRow("recommend","26921092")
//    this.getAllRows("recommend")
//      this.getOneRealRecommendRow("real_recommend",26921092)
//    println("26921092".getBytes())



  }


}

