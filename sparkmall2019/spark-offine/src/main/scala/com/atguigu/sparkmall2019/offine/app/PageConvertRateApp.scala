package com.atguigu.sparkmall2019.offine.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall2019.common.model.UserVisitAction
import com.atguigu.sparkmall2019.offine.resultmodel.PageConvertRate
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by kelvin on 2019/6/18.
  */
object PageConvertRateApp {



  def main(args: Array[String]): Unit = {
    println(Array(1, 2, 3).slice(0, 2).mkString(","))// 前包后不包
  }
  def calcPageConvertRate(sparkSession: SparkSession, taskId: String, condition: String, userVisitActionRDD: RDD[UserVisitAction]): List[PageConvertRate] = {

      //单跳转化率 = 从上页面跳转到当前页面的次数  / 上一个页面的访问次数
    //上一个页面的访问次数
    //      按照给定的目标页面去过滤 => 所有目标页面的访问记录 => 按照页面编号进行聚合,统计 =>
    val conditionObj: JSONObject = JSON.parseObject(condition)

    val pageFlowArray: Array[String] = conditionObj.getString("targetPageFlow").split(",") //Array(1,2,3,4,5,6,7)
    val pageFlowArrayBC=sparkSession.sparkContext.broadcast(pageFlowArray.slice(0,6)) //Array(1,2,3,4,5,6)

    //所有目标页面的访问记录
    //按照页面编号进行聚合,统计
    val pageCountMap: collection.Map[Long, Long] = userVisitActionRDD.filter(userVisitAction =>
      pageFlowArrayBC.value.contains(userVisitAction.page_id.toString)
    ) //剩下所有符合目标页面的访问记录
      .map(userVisitAction => (userVisitAction.page_id, 1L)).countByKey()
    //每个目标页面的访问次数(pageId,count)
    //从上页面跳转到当前页面的次数
    //从目标页面变换跳转的结构 => 1-2 ,2-3,3-4,4-5,5-6,6-7
    val pageFlowArrayPrefix: Array[String] = pageFlowArray.slice(0, 6) //1-6

    val pageFlowArraySuffix: Array[String] = pageFlowArray.slice(1,7) //2-7

    val tuples: Array[(String, String)] = pageFlowArrayPrefix.zip(pageFlowArraySuffix)//Array((1,2)(2,3)(3,4)(4,5)(5,6)(6,7))
    val targetPageJumps: Array[String] = tuples.map { case (prefix, suffix) => prefix + "_" + suffix } //Array(1-2,2-3,3-4,4-5,5-6,6-7)
    val targetPageJumpsBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageJumps)

    //把所有访问记录变成跳转对:
    val sessionActionsRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map { userVisitAction => (userVisitAction.session_id, userVisitAction)
    }.groupByKey()

    val pageJumpsRDD: RDD[(String, Long)] = sessionActionsRDD.flatMap { case (sessionId, actionItr) =>
      val actionsSorted: List[UserVisitAction] = actionItr.toList.sortWith { (action1, action2) =>
        val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        formator.parse(action1.action_time).getTime < formator.parse(action2.action_time).getTime //前小后大 就是正序
      }
      val actionZip: List[(UserVisitAction, UserVisitAction)] = actionsSorted.slice(0, actionsSorted.length - 1).zip(actionsSorted.slice(1, actionsSorted.length))
      //Array((1,2),(2,8),(8,10)
      val sessionPageJumps: List[String] = actionZip.map {
        case (action1, action2) => action1.page_id + "_" + action2.page_id
      }
      //Array(1-2,2-8,8-10)
      val sessionPageJumpsFiltered: List[(String, Long)] = sessionPageJumps.filter(pageJumps => targetPageJumpsBC.value.contains(pageJumps))
        .map { pageJump => (pageJump, 1L) } //Array((1-2,1L)) 有的是Array((1-2,1L),(2-3,1L),(3-4,1L)
      sessionPageJumpsFiltered
    } //RDD((1-2,1L),(1-2,1L),(2-3,1L).....)

    val pageJumpsCountMap: collection.Map[String, Long] = pageJumpsRDD.countByKey()  //每个目标页面跳转次数(pageJumps,count) RDD(1-2,10000),(2-3,5000),(3-4,4000)....(6-7,400))

    //把用的session为单位进行聚合,按访问时间排序(sessionId,iterable(Action))
    //(sessionId,Array(1-3,3-4,4-5))=>全用户的跳转记录 => 跟目标页面跳转进行过滤
    //得到所有用户全部的目标页面跳转集合 => 按照页面跳转进行聚合>> (page_jump,count)
    //遍历跳转页面的次数统计,利用单页的访问次数  进行除法  过的单跳转化率
    val pageConvertRateList: List[PageConvertRate] = pageJumpsCountMap.map {
      case (pageJump, count) =>
        val fromPage: String = pageJump.split("_")(0)
        val fromPageCount: Long = pageCountMap.get(fromPage.toLong).get
        val pageJumpRate: Double = Math.round(count / fromPageCount.toDouble * 1000) / 10.0
        PageConvertRate(taskId, pageJump, pageJumpRate)
    }.toList
    pageConvertRateList

  }

}
