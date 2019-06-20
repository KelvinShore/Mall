package com.atguigu.sparkmall2019.offine.app

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall2019.common.model.UserVisitAction
import com.atguigu.sparkmall2019.common.util.ConfigurationUtil
import com.atguigu.sparkmall2019.offine.resultmodel._
import com.atguigu.sparkmall2019.offine.util.SessionStatAccumulate
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

/**
  * Created by kelvin on 2019/6/18.
  */
object OffLineStatApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OffLine").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val taskId = UUID.randomUUID().toString
    val conditionConfig = ConfigurationUtil("condition.properties").config
    val conditions = conditionConfig.getString("condition.params.json")
    //声明 注册累加器
    val sessionStatAccumulate = new SessionStatAccumulate
    sparkSession.sparkContext.register(sessionStatAccumulate)

    /*   取出数据 过滤*/
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession)
    //转换成 sessionid 的 k v    变成一个sessionId 对应多个该sessionId的访问记录
    val userVisitActionSessionIdRDD: RDD[(String, UserVisitAction)] = userVisitActionRDD.map {
      userVisitAction =>
        (userVisitAction.session_id, userVisitAction)
    }
    //按照sessionId进行聚合groupbykey  sessionId,Itr(action1,action2,action3)

    val sessionActionItrRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionSessionIdRDD.groupByKey()

    //==>sessionid,sessionInfo      遍历所有session  逐个统计每个session信息
    val sessionInfoRDD: RDD[(String, SessionInfo)] = sessionActionItrRDD.map {
      case (sessionId, actionItr) =>
        var maxActionTime = 0L
        var minActionTime = 0L

        val searchkeywords = ListBuffer[String]()
        val clickProductIds = ListBuffer[String]()
        val orderProductIds = ListBuffer[String]()
        val payProductIds = ListBuffer[String]()

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        for (userVisitAction <- actionItr) {
          val actionTime: Date = format.parse(userVisitAction.action_time)
          val actionTimeMilSec: Long = actionTime.getTime

          if (minActionTime == 0L || actionTimeMilSec < minActionTime) {
            minActionTime = actionTimeMilSec
          }

          if (maxActionTime == 0L || actionTimeMilSec > maxActionTime) {
            maxActionTime = actionTimeMilSec
          }

          //合并相关信息
          if (userVisitAction.search_keyword != null) searchkeywords += userVisitAction.search_keyword
          if (userVisitAction.click_product_id > 0) clickProductIds += userVisitAction.click_product_id.toString
          if (userVisitAction.order_product_ids != null) orderProductIds += userVisitAction.order_product_ids
          if (userVisitAction.pay_product_ids != null) payProductIds += userVisitAction.pay_product_ids

        }
        val visitLength = (maxActionTime - minActionTime) / 1000
        println(s"visitLength = ${visitLength}")

        val stepLength = actionItr.size
        val startTimeString: String = format.format(new Date(minActionTime))

        val sessionInfo = SessionInfo(taskId, sessionId, visitLength, stepLength, startTimeString, searchkeywords.mkString(","), clickProductIds.mkString(","), orderProductIds.mkString(","), payProductIds.mkString(","))

        (sessionId, sessionInfo)
    }
    sessionInfoRDD.cache() //被需求二公用
    //需求一: 各个范围session步长,访问时长占比统计
    import  sparkSession.implicits._
    val sessionStatRDD: RDD[SessionStat] = SessionStatApp.statSessionRatio(sparkSession,taskId,conditions,sessionStatAccumulate,sessionInfoRDD)
    //插入到mysql中
    insertMysql(sparkSession,sessionStatRDD.toDF(),"session_stat")
    println("需求一:各个范围session步长,访问时长占比统计  完成!!!")
    //需求二: 按每小时session数量比例随机抽取1000个session
    val sessionRDD: RDD[SessionInfo] = SessionRandomExtractor.extractSessions(sparkSession,sessionInfoRDD)
    //存表
    insertMysql(sparkSession,sessionRDD.toDF(),"random_session_info")
    println("需求二:按每小时session数量比例随机抽取1000个session  完成!!!")

    //需求三: Top10热门品类 获取点击,下单 和支付数量排名前10的品类
    val categoryCountMap: mutable.HashMap[String, Long] = CategoryCountApp.categoryCount(sparkSession,userVisitActionRDD)

    val categoryActionMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy {
      case (cid_actintype, count) =>
        val cid = cid_actintype.split("_")(0)
        cid
    }
    val categoryTopNItr: immutable.Iterable[CategoryTopN] = categoryActionMap.map {
      case (cid, categoryActionCountMap) =>
        val clickCount: Long = categoryActionCountMap.getOrElse(cid + "_click", 0L)
        val orderCount: Long = categoryActionCountMap.getOrElse(cid + "_order", 0L)
        val payCount: Long = categoryActionCountMap.getOrElse(cid + "_pay", 0L)

        CategoryTopN(taskId, cid, clickCount, orderCount, payCount)
    }
    //排序  截取
    println(categoryTopNItr.mkString("\n"))

    val categoryTop10List: List[CategoryTopN] = categoryTopNItr.toList.sortWith {
      (categoryTopN1, categoryTopN2) =>
        if (categoryTopN1.click_count < categoryTopN2.click_count) {
          true
        } else if (categoryTopN1.click_count == categoryTopN2.click_count) {
          if (categoryTopN1.order_count < categoryTopN1.order_count) {
            true
          } else {
            false
          }
        } else {
          false
        }
    }.take(10)

    val dataFrame: DataFrame = sparkSession.sparkContext.makeRDD(categoryTop10List).toDF()
    insertMysql(sparkSession,dataFrame,"category_top10")
    println("需求三:Top10热门品类 获取点击,下单 和支付数量排名前10的品类 完成!!!")

    //需求四 Top10热门品类中Top10活跃Session统计  对于排名前10的品类,分别获取其点击次数排名前10的sessionId
    val top10SessionPerCidRDD: RDD[TopSessionPerCid] = TopSessionPerTopCategoryApp.getTopSession(sparkSession,taskId,categoryTop10List,userVisitActionRDD)

    insertMysql(sparkSession,top10SessionPerCidRDD.toDF(),"top10_session_per_top10_cid")

    println("需求四: Top10热门品类中Top10活跃Session统计  对于排名前10的品类,分别获取其点击次数排名前10的sessionId 完成!!!")

    //需求五: 页面单跳转率统计
    val pageConvertRates: List[PageConvertRate] = PageConvertRateApp.calcPageConvertRate(sparkSession,taskId,conditions,userVisitActionRDD)
    val pageConvertRatesRDD: RDD[PageConvertRate] = sparkSession.sparkContext.makeRDD(pageConvertRates)
    insertMysql(sparkSession,pageConvertRatesRDD.toDF(),"page_convert_rate")
    println("需求五: 页面单跳转率统计 完成!!!")







  }












  /**
    * 将数据插入指定的数据库表中
    * @param session
    * @param dataFrame
    * @param tableName
    */
  def insertMysql(session: SparkSession, dataFrame: DataFrame, tableName: String):Unit={
    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config

    dataFrame.write.format("jdbc")
          .option("url",config.getString("jdbc.url"))
      .option("user",config.getString("jdbc.user"))
      .option("password",config.getString("jdbc.password"))
        .mode(SaveMode.Append)
          .option("dbtable",tableName).save()


  }











  /*
        RDD[(sessionId,action1)]


        每个session的时长 步长-> 经过分类累加 （累加器）sessionId,(step_length,visit_length)
        每种占比的个数 除以总session数->
        步长 时长占比

        (
          "大于5秒" ,1000
          小于5秒” 500
        大于5次  700
        小于5次   800
        总数     1500


        )
        大于5秒 /总数    小于5秒/总数  大于5次/总数
    */

  /**
    * 从hive中获取访问日志  并根据条件过滤  生成RDD
    * @param sparkSession
    * @return
    */
  def readUserVisitActionRDD(sparkSession: SparkSession)={

    val configProperties = ConfigurationUtil("config.properties").config
    val conditionConfig = ConfigurationUtil("condition.properties").config
    val conditionJsonString = conditionConfig.getString("condition.params.json")
    val conditionJSonObj = JSON.parseObject(conditionJsonString)

    val sql = new StringBuilder("select ua.* from user_visit_action ua join user_info ui on ua.user_id=ui.user_id")
    sql.append(" where 1=1 ")
    if(conditionJSonObj.getString("startDate")!=""){
      sql.append(" and date>='"+conditionJSonObj.getString("startDate")+"'")
    }
    if(conditionJSonObj.getString("endDate")!=""){
      sql.append(" and date<='"+conditionJSonObj.getString("endDate")+"'")
    }
    if(conditionJSonObj.getString("startAge")!=""){
      sql.append(" and age>="+conditionJSonObj.getString("startAge"))
    }
    if(conditionJSonObj.getString("endAge")!=""){
      sql.append(" and age<="+conditionJSonObj.getString("endAge"))
    }
    if(conditionJSonObj.getString("professionals")!=""){
      sql.append(" and professionals="+conditionJSonObj.getString("professionals"))
    }
    if(conditionJSonObj.getString("gender")!=""){
      sql.append(" and gender="+conditionJSonObj.getString("gender"))
    }
    if(conditionJSonObj.getString("city")!=""){
      sql.append(" and city_id="+conditionJSonObj.getString("city"))
    }
    val database = configProperties.getString("hive.database")
    if(database!=null){
      sparkSession.sql("use "+database)
    }
    println(s"sql.toString() = ${sql.toString()}")
    val dataFrame = sparkSession.sql(sql.toString())
    dataFrame.show(100)
    import sparkSession.implicits._
    dataFrame.as[UserVisitAction].rdd


  }



}
