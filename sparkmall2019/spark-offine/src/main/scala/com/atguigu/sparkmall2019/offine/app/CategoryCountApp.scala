package com.atguigu.sparkmall2019.offine.app

import com.atguigu.sparkmall2019.common.model.UserVisitAction
import com.atguigu.sparkmall2019.offine.util.CategoryActionCountAccumulate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by kelvin on 2019/6/18.
  */
object CategoryCountApp {
  def categoryCount(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction]): mutable.HashMap[String,Long] = {
      val categoryActionCountAccumulate = new CategoryActionCountAccumulate
      sparkSession.sparkContext.register(categoryActionCountAccumulate)

      userVisitActionRDD.foreach{
        action =>
          //根据操作类型不同,分布进行计数
          if(action.click_category_id > 0){
            categoryActionCountAccumulate.add(action.click_category_id.toString+"_"+"click")
          }else if(action.order_category_ids != null){
            val orderCids: Array[String] = action.order_category_ids.split(",")
            for(orderCid <- orderCids){
              categoryActionCountAccumulate.add(orderCid +"_"+"order")
            }

          }else if(action.pay_category_ids != null){
            val payCids: Array[String] = action.pay_category_ids.split(",")
            for(payCid <- payCids){
              categoryActionCountAccumulate.add(payCid+"_"+"pay")
            }

          }
      }
    println(categoryActionCountAccumulate.value.mkString(","))
    categoryActionCountAccumulate.value
  }


}
