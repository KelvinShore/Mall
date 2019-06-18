package com.atguigu.sparkmall2019.mock.util

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by kelvin on 2019/6/17.
  */
object RandomOptions {

  def apply[T](opts:RanOpt[T]*):RandomOptions[T]={
    val randomOptions = new RandomOptions[T]()

    for (opt <- opts){
      randomOptions.totalWeight += opt.weight
      for(i <- 1 to opt.weight){
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }


}
case class RanOpt[T](value:T,weight:Int){

}
class RandomOptions[T](opts:RanOpt[T]*){

  var totalWeight=0
  var optsBuffer=new ListBuffer[T]

  def getRandomOpt():T={
    val randomNum=new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }

}