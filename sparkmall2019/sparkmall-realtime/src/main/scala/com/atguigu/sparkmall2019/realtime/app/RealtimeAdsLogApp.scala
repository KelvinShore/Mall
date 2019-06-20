package com.atguigu.sparkmall2019.realtime.app
import java.util
import java.util.Date
import javax.security.auth.login.Configuration

import com.atguigu.sparkmall2019.common.util.{KafkaUtil, RedisUtil}
import com.atguigu.sparkmall2019.realtime.model.RealtimeAdsLog
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

/**
  * Created by kelvin on 2019/6/19.
  */
object RealtimeAdsLogApp {

  def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("realtime_log").setMaster("local[*]")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        val ssc = new StreamingContext(sparkSession.sparkContext,Seconds(5))

        sparkSession.sparkContext.setCheckpointDir("./checkpoint")

        val inputDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log",ssc)

        //获得业务数据
       val realtimeLogLineDStream: DStream[String] = inputDstream.map{ record => record.value()}

        //把一行的文本日志转化为class
    val realtimeLogDstream: DStream[RealtimeAdsLog] = realtimeLogLineDStream.map { line =>

      val logArray: Array[String] = line.split(" ")
      val milsec: Long = logArray(0).toLong
      RealtimeAdsLog(new Date(milsec), logArray(1), logArray(2), logArray(3), logArray(4))
    }
    realtimeLogDstream.cache()

    realtimeLogLineDStream.foreachRDD{rdd => rdd}

    val jedis: Jedis = RedisUtil.getJedisClient

    val realtimeFilteredLogDstream: DStream[RealtimeAdsLog] = realtimeLogDstream.transform { rdd =>
      //获取黑名单
      val blackList: util.Set[String] = jedis.smembers("blackList")

      println("!redis查询黑名单" + blackList)
      val blackListBC: Broadcast[util.Set[String]] = sparkSession.sparkContext.broadcast(blackList)

      rdd.filter { realtimeLog => !blackListBC.value.contains(realtimeLog.userId) }

    }

    //统计之前过滤掉黑名单中的用户
    //统计每天  每个用户点击某个广告的次数
    //判断 这天累计次数达到100次的用户,保存的黑名单中(redis记录每人每天每广告点击次数,记录黑名单的userid)

    realtimeFilteredLogDstream.foreachRDD{rdd => rdd.collect()

        rdd.foreachPartition{realtimeLogItr =>

          val jedis: Jedis = RedisUtil.getJedisClient

          //进行计数
          val pipeline: Pipeline = jedis.pipelined()

          for(realtimeLog <- realtimeLogItr){
            //相同的key+1
            pipeline.hincrBy("user_count_ads_perday",realtimeLog.toUserCountPerdayKey(),1L)

            val count: String = jedis.hget("user_count_ads_perday",realtimeLog.toUserCountPerdayKey())

            //判断是否进黑名单
            if(count.toInt >= 10000){
              pipeline.sadd("blackList",realtimeLog.userId)
            }

          }
          pipeline.sync()
          jedis.close()

        }

    }

    //需求八
    val areaCityTotalCountDStream: DStream[(String, Long)] = AreaCityAdsCountPerDayApp.adsCount(realtimeLogDstream)

    areaCityTotalCountDStream.foreachRDD(rdd =>
      println(rdd.collect().mkString("\n"))
    )

    areaCityTotalCountDStream.foreachRDD{rdd =>
      rdd.foreachPartition{areaCityTotalCountItr =>
        val jedis: Jedis = RedisUtil.getJedisClient

        println(s"areaCityTotalCountItr=${areaCityTotalCountItr}")

        for((areaCityCountKey,count) <- areaCityTotalCountItr){
          jedis.hset("area_city_ads_count",areaCityCountKey,count.toString)
        }
        println(s"areaCityTotalCountItr=${areaCityTotalCountItr.size}")
        jedis.close()

      }

    }

    //需求九
    AreaTop3AdsCountPerDay.adsCount(areaCityTotalCountDStream).foreachRDD(rdd =>{

      val areaAdsCountPerDayArr: Array[(String, Map[String, String])] = rdd.collect()

      for ((dateKey,areaTop3AdsMap) <- areaAdsCountPerDayArr){

        import  scala.collection.JavaConversions._
        jedis.hmset(dateKey,areaTop3AdsMap)

      }

    })

    //需求十
    val lastHourMinuCountJsonPerAdsDstream: DStream[(String, String)] = AdsCountPerHourApp.adsCount(realtimeLogDstream)

    lastHourMinuCountJsonPerAdsDstream.foreachRDD{ rdd =>
      val lastHourMap:Map[String,String]  = rdd.collect().toMap

      import scala.collection.JavaConversions._
      jedis.del("last_hour_ads_click")

      jedis.hmset("last_hour_ads_click",lastHourMap)
    }
    ssc.start()
    ssc.awaitTermination()
    jedis.close()

    }

}
