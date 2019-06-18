package com.atguigu.sparkmall2019.mock.util

import java.util.Date

import scala.util.Random

/**
  * Created by kelvin on 2019/6/17.
  */
object RandomDate {

  def apply(startDate:Date,endDate:Date,step:Int):RandomDate={
      val randomDate = new RandomDate()

      val avgStepTime: Long = (endDate.getTime - startDate.getTime)/step

      randomDate.maxTimeStep = avgStepTime*2
      randomDate.lastDateTime=startDate.getTime

      randomDate
  }


}
class RandomDate{

  var lastDateTime=0L
  var maxTimeStep=0L

  def getRandomDate()={
    val timeStep: Int = new Random().nextInt(maxTimeStep.toInt)

    lastDateTime = lastDateTime +timeStep

    new Date(lastDateTime)
  }



}