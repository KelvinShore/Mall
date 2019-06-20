package com.atguigu.sparkmall2019.realtime.app

import com.alibaba.fastjson.JSON
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by kelvin on 2019/6/20.
  */
object AreaTop3AdsCountPerDay {
  def adsCount(areaCityAdsCountDstream:DStream[(String,Long)]):DStream[(String,Map[String,String])]= {
    //把同一地区但是不同城市的数据加起来
    val areaAdsCountDstrem: DStream[(String, Long)] = areaCityAdsCountDstream.map { case (dateAreaCityAdsKey, count) =>
      val keyArr: Array[String] = dateAreaCityAdsKey.split("_")
      val date: String = keyArr(0)
      val area: String = keyArr(1)
      val ads: String = keyArr(3)
      (date + "_" + area + "_" + ads, count)
    }
      .reduceByKey(_ + _)
    //按地区进行聚合
    val areaTop3ItrPerDayDstream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCountDstrem.map { case (dateAreaAdsKey, count) =>
      val keyArr: Array[String] = dateAreaAdsKey.split("_")
      val date: String = keyArr(0)
      val area: String = keyArr(1)
      val ads: String = keyArr(2)
      ("area_top3_ads:" + date, (area, (ads, count)))
    }.groupByKey()
    // 把每个地区的广告计数集合 转换成广告计数的json
    val areaTop3AdsJsonPerDayDstream: DStream[(String, Map[String, String])] = areaTop3ItrPerDayDstream.map { case (dateKey, areaItr) =>

      val areaMap: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (ads, count)) => area }
      val areaTop3AdsJsonMap: Map[String, String] = areaMap.map { case (area, areaAdsItr) =>
        val adsTop3List: List[(String, Long)] = areaAdsItr.map { case (area, (ads, count)) => (ads, count) }
          .toList.sortWith { (adscount1, adscount2) => adscount1._2 > adscount2._2 }.take(3)
        val top3AdsJson: String = compact(render(adsTop3List))
        (area, top3AdsJson)
      }

      (dateKey, areaTop3AdsJsonMap)
    }
    areaTop3AdsJsonPerDayDstream
  }
}
