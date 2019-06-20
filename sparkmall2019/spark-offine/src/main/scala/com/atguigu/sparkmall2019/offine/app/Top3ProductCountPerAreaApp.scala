package com.atguigu.sparkmall2019.offine.app

import java.util.UUID

import com.atguigu.sparkmall2019.offine.util.UdfGroupbyCityCount
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by kelvin on 2019/6/19.
  */
object Top3ProductCountPerAreaApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("OffLine").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    sparkSession.udf.register("groupby_city_count",new UdfGroupbyCityCount())

    val taskId: String = UUID.randomUUID().toString

    //商品表  城市表  访问记录表
    //地区表和访问纪律表表关联  =>> 带地区的访问记录
    sparkSession.sql("use sparkmall2019")

   val df: DataFrame = sparkSession.sql("select click_product_id,ci.area,ci.city_id,ci.city_name from user_visit_action ua join city_info ci on ua.city_id=ci.city_id where click_product_id>0")

    df.createOrReplaceTempView("tmp_area_product_click")

    //  ==>> 以地区+商品ID作为key,count出来点击次数
    val areaProductCountDF: DataFrame = sparkSession.sql("select  area,click_product_id, count(*) clickcount,groupby_city_count(city_name) city_remark  from tmp_area_product_click group by area ,click_product_id")

    areaProductCountDF.createOrReplaceTempView("area_product_count")

    // ==>> 利用开窗函数进行分组排序  =>> 截取所有分组中前三名
    val areaProductCountRankDF: DataFrame = sparkSession.sql("select * ,rank()over(partition by area order by clickcount desc)rk from area_product_count")

    areaProductCountRankDF.createOrReplaceTempView("area_product_count_rank")

    sparkSession.sql("select ar.area,pi.product_name,ar.clickcount,ar.rk,ar.city_remark from area_product_count_rank ar join product_info pi on pi.product_id=ar.click_product_id  where rk<=3").show(100,false)


  }


}
