package com.atguigu.sparkmall2019.realtime.app

import java.text.SimpleDateFormat

import com.atguigu.sparkmall2019.realtime.model.RealtimeAdsLog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by kelvin on 2019/6/20.
  */
object AdsCountPerHourApp {

  def adsCount(realtimeLogDstream:DStream[RealtimeAdsLog]):DStream[(String,String)]={
    //      1、	窗口函数 => 每隔多长时间，截取到最近一小时的所有访问记录
    //      2、	 rdd放了某段时间的访问记录  RDD[RealtimeAdsLog]
    val lastHourAdsLogDStream: DStream[RealtimeAdsLog] = realtimeLogDstream.window(Minutes(60),Seconds(10))
    //        RDD（广告id_小时分钟，1L）=>reducebykey
    //      变成  广告id+小时分钟作为key，进行聚合
    val lastHourAdsMinuCountDstream: DStream[(String, Long)] = lastHourAdsLogDStream.map { adslog =>
      val hourMinu: String = new SimpleDateFormat("HH:mm").format(adslog.date)
      (adslog.adsId + "_" + hourMinu, 1L)
    }.reduceByKey(_ + _)
    //      => RDD（广告id_小时分钟，count）
    //      =>RDD(广告，（小时分钟，count）) =>groupbykey
    val lastHourMinuCountPerAdsDstream: DStream[(String, Iterable[(String, Long)])] = lastHourAdsMinuCountDstream.map { case (adsMinuKey, count) =>
      val keyArr: Array[String] = adsMinuKey.split("_")
      val ads: String = keyArr(0)
      val hourMinu: String = keyArr(1)
      (ads, (hourMinu, count))
    }.groupByKey()
    lastHourMinuCountPerAdsDstream

    //      =>RDD（广告，iterable(小时分钟，count)）
    //      =>RDD（广告，iterable(小时分钟，count)）排序序列化 转成json .collect
    val lastHourMinuCountJsonPerAdsDstream: DStream[(String, String)] = lastHourMinuCountPerAdsDstream.map { case (ads, minuItr) =>
      val hourminuCountJson: String = compact(render(minuItr))
      (ads, hourminuCountJson)
    }
    lastHourMinuCountJsonPerAdsDstream


  }
}
