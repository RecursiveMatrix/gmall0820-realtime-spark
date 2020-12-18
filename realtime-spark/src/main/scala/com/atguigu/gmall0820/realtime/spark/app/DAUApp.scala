package com.atguigu.gmall0820.realtime.spark.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0820.realtime.spark.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DAUApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf,Duration(5000))
    val groupId = "dau_app_group"
    val topic = "ODS_BASE_LOG"

    val inputRecordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
//    inputRecordDStream.map(_.value()).print()
    val jsonDStream = inputRecordDStream.map {
      record =>
        val jsonObject = JSON.parseObject(record.value())
        val ts = jsonObject.getLong("ts")
        val dateformat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateTimeStr = dateformat.format(new Date(ts))
        val dateTimeArr = dateTimeStr.split(" ")
        jsonObject.put("dt", dateTimeArr(0))
        jsonObject.put("hr", dateTimeArr(1))
        jsonObject

    }
    val firstVisitDstream: DStream[JSONObject] = jsonDStream.filter(jsonObj => {
      val pageJson = jsonObj.getJSONObject("page")
      if (pageJson != null) {
        val lastPageId = pageJson.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          true
        }else{
          false
        }
      }else{
        false
      }
    })

    val dauJsonObjDStream = firstVisitDstream.mapPartitions { jsonItr =>
      val jedis = RedisUtil.getJedisClient
      val listBuffer = new ListBuffer[JSONObject]()
      for (jsonObj <- jsonItr) {
        val mid = jsonObj.getJSONObject("common").getString("mid")
        val dt = jsonObj.getString("dt")
        val dauKey = "dau:" + dt
        val nonExits = jedis.sadd(dauKey, mid)
        jedis.expire(dauKey, 24 * 3600)
        if (nonExits == 1) {
          listBuffer.append(jsonObj)
        }
      }
      jedis.close()
      listBuffer.toIterator
    }
    dauJsonObjDStream.print(100)



    ssc.start()
    ssc.awaitTermination()
  }
}
