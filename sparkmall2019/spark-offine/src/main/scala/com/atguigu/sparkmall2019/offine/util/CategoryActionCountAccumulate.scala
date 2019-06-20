package com.atguigu.sparkmall2019.offine.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by kelvin on 2019/6/18.
  */
class CategoryActionCountAccumulate extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

   var categoryCountMap = new mutable.HashMap[String,Long]()

  //判断累加器是否为初始值
  override def isZero: Boolean = {
    categoryCountMap.size==0
  }

  //复制一份内容一样的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val newAccumulator = new CategoryActionCountAccumulate()

    newAccumulator.categoryCountMap ++= categoryCountMap
    newAccumulator
  }

  //重置累加器
  override def reset(): Unit = {
    categoryCountMap = new mutable.HashMap[String,Long]()
  }

  //根据传入的key 进行对应的累加
  override def add(key: String): Unit = {
    categoryCountMap(key)= categoryCountMap.getOrElse(key,0L)+1L
  }

  //两个累加器合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    other match
      {
      case otherAccumulate:CategoryActionCountAccumulate =>
        otherAccumulate.categoryCountMap.foldLeft(categoryCountMap){case (sMap,(key,count))=>
          sMap(key)=sMap.getOrElse(key,0L)+count
            sMap
        }
    }
  }

  //返回最终累加好的map
  override def value: mutable.HashMap[String, Long] = {
    categoryCountMap
  }
}
