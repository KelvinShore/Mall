package com.atguigu.sparkmall2019.realtime.model

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by kelvin on 2019/6/19.
  */
case class RealtimeAdsLog (date:Date,area:String,city:String,userId:String,adsId:String){

  def toUserCountPerdayKey():String={
    val perDay: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    perDay+"_"+userId+"_"+adsId
  }

  def toAreaCityCountPerdayKey():String={
    val perDay: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    perDay+"_"+area+"_"+city+"_"+adsId
  }
}
