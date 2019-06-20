package com.atguigu.sparkmall2019.realtime.app

import com.atguigu.sparkmall2019.realtime.model.RealtimeAdsLog
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by kelvin on 2019/6/20.
  */
object AreaCityAdsCountPerDayApp {

  def adsCount(realtimeDStream:DStream[RealtimeAdsLog]):DStream[(String,Long)]={
    //DStream[Realtimelog] =>
    //转化成DStream[(date:area:city:ads,1L)]

    val areaCityClickDStream: DStream[(String, Long)] = realtimeDStream.map{realtimelog => (realtimelog.toAreaCityCountPerdayKey(),1l)}

    //进行reduceByKey() 每5秒的所有key累计值
    val areaCityCountDStream: DStream[(String, Long)] = areaCityClickDStream.reduceByKey(_+_)

    //updateStateByKey 来汇总到历史计数中
    val areaCityTotalCountDStream: DStream[(String, Long)] = areaCityCountDStream.updateStateByKey { (values: Seq[Long], sumOption: Option[Long]) =>

      println(s"values.sum = ${values.mkString("||")}")
      val thisSum: Long = values.sum
      var sumValue: Long = sumOption.getOrElse(0L)
      if(values.sum!=null){
        sumValue += thisSum
      }
      Some(sumValue)
    }

    areaCityTotalCountDStream
  }

}
