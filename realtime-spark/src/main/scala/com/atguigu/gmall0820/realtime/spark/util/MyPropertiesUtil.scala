package com.atguigu.gmall0820.realtime.spark.util

import java.io.InputStreamReader
import java.util.Properties

// 专门用来读配置文件
object MyPropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties =  MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

}
