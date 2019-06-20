package com.atguigu.sparkmall2019.offine.app

import com.atguigu.sparkmall2019.common.model.UserVisitAction
import com.atguigu.sparkmall2019.offine.resultmodel.{CategoryTopN, TopSessionPerCid}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by kelvin on 2019/6/18.
  */
object TopSessionPerTopCategoryApp {



  def getTopSession(sparkSession: SparkSession, taskId: String, categoryTop10List: List[CategoryTopN], userVisitActionRDD: RDD[UserVisitAction]): RDD[TopSessionPerCid] = {

    val topCategoryListBC: Broadcast[List[CategoryTopN]] = sparkSession.sparkContext.broadcast(categoryTop10List)

    //过滤掉不属于top10品类的action
    val topCidActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter {
      userVisitAction =>
        //把列表中的对象  转换成计数
        val topcidList: List[Long] = topCategoryListBC.value.map {
          categoryTopN => categoryTopN.category_id.toLong
        }
        println(topcidList.mkString("-") + "|||" + userVisitAction.click_category_id)
        topcidList.contains(userVisitAction.click_category_id)
    }
    topCidActionRDD.foreach(println(_))

    //按照 品类+sessionid为key 进行聚合(品类id_sessionId,count)
    val topCidSessionClickRDD: RDD[(String, Long)] = topCidActionRDD.map(action => (action.click_category_id+"_"+action.session_id,1L))

    val topCidSessionCountRDD: RDD[(String, Long)] = topCidSessionClickRDD.reduceByKey(_+_)

    //转换这个集合
    //(cid,TopSession(sessionid,count))=>聚合
    val topSessionPerCidsRDD: RDD[(String, TopSessionPerCid)] = topCidSessionCountRDD.map {
      case (topcidSessionId, count) =>
        val topcidSessionIdArr = topcidSessionId.split("_")
        val topcid = topcidSessionIdArr(0)
        val sessionId = topcidSessionIdArr(1)
        (topcid, TopSessionPerCid(taskId, topcid, sessionId, count))

    }
    val topSessionItrPerCidsRDD: RDD[(String, Iterable[TopSessionPerCid])] = topSessionPerCidsRDD.groupByKey()

    //cid,iterable(TopSession)
    //针对这个iterable进行排序 截取前十
    val top10SessionPerCidRDD: RDD[TopSessionPerCid] = topSessionItrPerCidsRDD.flatMap {
      case (cid, topsessionItr) =>
        val top10SessionList: List[TopSessionPerCid] = topsessionItr.toList.sortWith {
          case (session1, session2) =>
            session1.clickCount > session2.clickCount
        }.take(10)
        top10SessionList
    }
    top10SessionPerCidRDD
    //List(TopSession 只有十个

  }


}
