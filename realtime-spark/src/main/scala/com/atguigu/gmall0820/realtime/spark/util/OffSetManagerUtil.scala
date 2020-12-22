package com.atguigu.gmall0820.realtime.spark.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffSetManagerUtil {

  def saveOffset(topic:String,group:String,offsetRanges: Array[OffsetRange])={
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic +":" + group
    val offsetMap = new util.HashMap[String,String]()
    for(offsetRange <- offsetRanges){
      val partition = offsetRange.partition.toString
      val untilOffset = offsetRange.untilOffset.toString
      println("写入偏移量：分区"+partition +":"+offsetRange.fromOffset+"-->"+untilOffset)
      offsetMap.put(partition,untilOffset)
    }
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }

  def getOffset(topic:String,group:String):Map[TopicPartition,Long]={

    val jedis = RedisUtil.getJedisClient
    // hash key: topic + group    filed: partition value: offset
    val offsetKey = topic+":"+group
    val offsetMap = jedis.hgetAll(offsetKey)
    jedis.close()

    import scala.collection.JavaConverters._
    val topicPartitionMap = offsetMap.asScala.map { case (partition, offset) =>
      val topicPartition = new TopicPartition(topic, partition.toInt)
      (topicPartition, offset.toLong)
    }.toMap
    topicPartitionMap
  }

}
