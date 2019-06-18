package com.atguigu.sparkmall2019.offine.app

import com.atguigu.sparkmall2019.offine.resultmodel.SessionInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by kelvin on 2019/6/17.
  */
object SessionRandomExtractor {

  val needSessionNum=1000

  def extractSessions(sparkSession:SparkSession,sessionInfoRDD:RDD[(String,SessionInfo)]):RDD[SessionInfo]={
    // 把action转换成session集合,把同一个session的相关数据进行拼接,放到一个字段中
    //得到所有的session集合
    //得到  按天小时为key的集合
    val sessionCount = sessionInfoRDD.count()

    val dayHourSessionRDD: RDD[(String, SessionInfo)] = sessionInfoRDD.map {
      case (sessionId, sessionInfo) =>
        val dateHourKey: String = sessionInfo.startTime.split(":")(0)
        (dateHourKey, sessionInfo)
    }
    //按照天+小时  进行groupbykey=> (天小时 -> session集合)
    val dayHourSessionsItrRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionRDD.groupByKey()

    //获得这个小时的session集合,按照某个小时需要session数量进行抽取
    //遍历所有小时集合
    val sessionInfoRandomRDD: RDD[SessionInfo] = dayHourSessionsItrRDD.flatMap { case (dayHour, sessionsItr) =>
      //计算某个小时要抽去多少个session
      //=   某个小时总session数量/日志总session数量 * 需要样板的session数量
      val dayhourSessionCount: Int = sessionsItr.size
      val dayhourNeedSessionNum: Double = dayhourSessionCount / sessionCount.toDouble * needSessionNum
      println(s"dayhourNeedSessionNum = ${dayhourNeedSessionNum}")
      val sessionList: List[SessionInfo] = randomExtract(sessionsItr.toArray, dayhourNeedSessionNum.toLong)

      sessionList
    }
    sessionInfoRandomRDD
    //只要得到这个小时的session集合
    //报存到数据库中
  }



  def randomExtract[T](array: Array[T],num:Long):List[T]={
    val hitIdxSet = new mutable.HashSet[Int]()
    val hitValue = new ListBuffer[T]

    while (hitValue.size < num){
      val ranIdx: Int = new Random().nextInt(array.size)
      if(!hitIdxSet.contains(ranIdx)){
        val value = array(ranIdx)
        hitValue += value
      }
    }
    hitValue.toList
  }






}
