package com.atguigu.gmall0820.realtime.spark.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0820.realtime.spark.bean.DauInfo
import com.atguigu.gmall0820.realtime.spark.util.{MyEsUtil, MyKafkaUtil, OffSetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer



object DAUApp {

  def main(args: Array[String]): Unit = {

    // TODO 1.spark 配置,ssc,消费topic,消费组
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf,Duration(5000))
    val groupId = "dau_app_group"
    val topic = "ODS_BASE_LOG"


    // TODO 2.获得当前偏移量 -- 每个分区的上次的消费位置
    val offsetMap: Map[TopicPartition, Long] = OffSetManagerUtil.getOffset(topic,groupId)
    // 如果没有偏移量，取默认的


    var recordInputDStream :InputDStream[ConsumerRecord[String,String]] = null
    if(offsetMap!=null&&offsetMap.size>0){
      recordInputDStream =MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }



    var offsetRanges: Array[OffsetRange] = Array.empty

    val recordInputWithOffsetDStream: DStream[ConsumerRecord[String, String]] = recordInputDStream.transform{
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "******")
        rdd
    }

    //    inputRecordDStream.map(_.value()).print()


    val jsonDStream: DStream[JSONObject] = recordInputWithOffsetDStream.map {
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


    // ?2 ----
      var ifFirst = false

    val firstVisitDstream: DStream[JSONObject] = jsonDStream.filter(jsonObj => {

      val pageJson = jsonObj.getJSONObject("page")

       // ?3  ---- 页面信息为空，null+length来判断不存在
      if (pageJson != null) {
        val lastPageId = pageJson.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          ifFirst = true
        }
        }
      ifFirst
    })


    val dauJsonObjDStream = firstVisitDstream.mapPartitions { jsonItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val sourceList: List[JSONObject] = jsonItr.toList
      val listBuffer = new ListBuffer[JSONObject]()
      println("before: "+ sourceList.size)

      for (jsonObj <- sourceList) {
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
      println("after: "+listBuffer.size)
      listBuffer.toIterator
    }
    dauJsonObjDStream.cache()
    dauJsonObjDStream.print(100)

    val dauInfoDStream = dauJsonObjDStream.map(jsonObj => {
      val commonObj = jsonObj.getJSONObject("common")
      DauInfo(commonObj.getString("mid"),
        commonObj.getString("uid"),
        commonObj.getString("ar"),
        commonObj.getString("ch"),
        commonObj.getString("vc"),
        jsonObj.getString("dt"),
        jsonObj.getString("hr"),
        System.currentTimeMillis())
    })
    dauInfoDStream.foreachRDD{rdd =>
      rdd.foreachPartition{ dauItr =>
        val dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val dateList =  dauItr.toList.map(dauInfo => (dauInfo,dauInfo.mid))
        MyEsUtil.saveBulkData(dateList,"gmall0820_dau_info_"+dt)
      }
      OffSetManagerUtil.saveOffset(topic,groupId,offsetRanges)
    }




    ssc.start()
    ssc.awaitTermination()
  }
}
