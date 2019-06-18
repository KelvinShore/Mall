package com.atguigu.sparkmall2019.offine.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by kelvin on 2019/6/17.
  */
class SessionStatAccumulate extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

 var sessionStatMap=new mutable.HashMap[String,Long]()

  //判断累加器是否为初始值
  override def isZero: Boolean = {
    sessionStatMap.size==0
  }

  //复制一份内容一样的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val newAccumulator = new SessionStatAccumulate()

    newAccumulator.sessionStatMap ++= sessionStatMap
    newAccumulator
  }

  //重置累加器
  override def reset(): Unit = {
    sessionStatMap = new mutable.HashMap[String,Long]()
  }
  //根据传入的key 进行对应的累加
  override def add(key: String): Unit = {
    sessionStatMap(key)=sessionStatMap.getOrElse(key,0L)+1L
  }
  //两个累加器合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    other match {
      case otherAccumulate:SessionStatAccumulate =>
                otherAccumulate.sessionStatMap.foldLeft(sessionStatMap){ case (sMap,(key,count)) =>
                  sMap(key)=sMap.getOrElse(key,0L)+count
                    sMap
                }
    }
  }
  //返回最终累加好的map
  override def value: mutable.HashMap[String, Long] = {
    sessionStatMap
  }
}
