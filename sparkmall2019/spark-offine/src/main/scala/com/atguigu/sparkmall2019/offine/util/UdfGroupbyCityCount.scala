package com.atguigu.sparkmall2019.offine.util


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

/**
  * Created by kelvin on 2019/6/19.
  */
class UdfGroupbyCityCount extends UserDefinedAggregateFunction{

  //输入字段声明
  override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))

  //声明存储的容器
  override def bufferSchema: StructType = StructType(Array(StructField("countMap",MapType(StringType,LongType)),StructField("countSum",LongType)))

  //输出类型
  override def dataType: DataType = StringType

  //一致性验证
  override def deterministic: Boolean = true

  //初始化容器
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=new HashMap[String,Long]()
    buffer(1)=0L
  }

  //更新数据  把传入值存入Map中
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val cityName: String = input.getString(0)
    val countMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val countSum: Long = buffer.getAs[Long](1)

    buffer(0)= countMap+(cityName -> (countMap.getOrElse(cityName,0L)+1L))
    buffer(1)=countSum+1L
  }

  //分区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val countMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    val countSum1: Long = buffer1.getAs[Long](1)
    val countMap2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
    val countSum2: Long = buffer2.getAs[Long](1)

    //合并两个分区
    buffer1(0) = countMap1.foldLeft(countMap2) {
      case (map2, (cityName, count)) => map2 + (cityName -> (map2.getOrElse(cityName, 0L) + count))
    }
    buffer1(1)=countSum1+countSum2

  }

  //显示最终结果
  override def evaluate(buffer: Row): String = {
    //取值 运算 做除法  List(CityRate(cityName,rate)).mkString,
    val countMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val countSum: Long = buffer.getAs[Long](1)

    val cityRateList = new ListBuffer[CityRate]()
    for((cityName,count) <- countMap){
      val rate: Double = Math.round(count/countSum.toDouble*1000)/10.0
      cityRateList.append(CityRate(cityName,rate))
    }
    //排序
    val cityRateSortedList: ListBuffer[CityRate] = cityRateList.sortWith{case (city1,city2) => city1.rate > city2.rate}

    val top2CityRate: ListBuffer[CityRate] = cityRateSortedList.take(2)

    //截取前2
    //计算其他
    var otherRate=100.0
    if (cityRateSortedList.size > 2){
      for (cityRate <- top2CityRate){
        otherRate -= cityRate.rate
      }
      top2CityRate.append(CityRate("其他",otherRate))
    }
    top2CityRate.mkString(",")
  }

}
case class CityRate(cityName:String,rate:Double){
  override def toString: String = {
    cityName+":"+rate+"%"
  }
}
