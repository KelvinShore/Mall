package com.atguigu.sparkmall2019.offine.app

import com.atguigu.sparkmall2019.offine.resultmodel.{SessionInfo, SessionStat}
import com.atguigu.sparkmall2019.offine.util.SessionStatAccumulate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by kelvin on 2019/6/17.
  */
object SessionStatApp {


  def statSessionRatio(sparkSession: SparkSession,taskId:String,conditions:String,sessionStatAccumulate: SessionStatAccumulate,sessionIdInfoRDD:RDD[(String,SessionInfo)])={
      //每个session的时长  步长 -> 经过分类累加(累加器)sessionId,(step_length,visit_length)
    //每种占比的个数  除以总session数 ->
    // 步长  时长占比
    sessionIdInfoRDD.foreach{case (sessionId,sessionInfo) =>

      if(sessionInfo.visitLength <= 10L){
        //累加
        sessionStatAccumulate.add("visit_length_le_10s")
      }else{
        //累加
        sessionStatAccumulate.add("visit_length_gt_10s")
      }

        if(sessionInfo.stepLength <= 5L){
          //累加
          sessionStatAccumulate.add("step_length_le5")
        }else{
          //累加
          sessionStatAccumulate.add("step_length_gt5")
        }
        //sessioncount累加
      sessionStatAccumulate.add("session_count")

    }
    //把累加器的值取出来  计算占比
    val sessionStatMap = sessionStatAccumulate.value

    val visitLenLe10: Long = sessionStatMap.getOrElse("visit_length_le_10s",0L)

    val visitLenGt10: Long = sessionStatMap.getOrElse("visit_length_gt_10s",0L)
    val stepLenLe5: Long = sessionStatMap.getOrElse("step_length_le_5",0L)
    val stepLenGt5: Long = sessionStatMap.getOrElse("vstep_length_gt_5",0L)
    val sessionCount: Long = sessionStatMap.getOrElse("session_count",0L)

    //计算占比
    val visitLenLe10Ratio: Double = Math.round(visitLenLe10 /sessionCount.toDouble*10000)/100.0
    val visitLenGt10Ratio: Double = Math.round(visitLenGt10 /sessionCount.toDouble*10000)/100.0
    val stepLenLe5Ratio: Double = Math.round(stepLenLe5 /sessionCount.toDouble*10000)/100.0
    val stepLenGt5Ratio: Double = Math.round(stepLenGt5 /sessionCount.toDouble*10000)/100.0

    //构造对象
    val sessionStat = SessionStat(taskId,conditions,sessionCount,visitLenLe10Ratio,visitLenGt10Ratio,stepLenLe5Ratio,stepLenGt5Ratio)
    val rdd: RDD[SessionStat] = sparkSession.sparkContext.makeRDD(Array(sessionStat))
    rdd

  }

}
