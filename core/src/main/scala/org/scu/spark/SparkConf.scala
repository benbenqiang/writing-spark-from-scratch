package org.scu.spark

import java.util.concurrent.ConcurrentHashMap

/**
 * Created by bbq on 2015/12/11
 */
class SparkConf extends Cloneable with Logging{
  private val settings = new ConcurrentHashMap[String,String]()

  set("spark.dirver.host","172.0.0.1")
  set("spark.driver.post","60001")


  def set(key:String,value:String):SparkConf={
    if(key == null || value == null){
      throw new NullPointerException(s"null value for key or value,key=$key value=$value")
    }
    settings.put(key,value)
    this
  }

  def get(key:String):String={
    Option(settings.get(key)).getOrElse(throw new NoSuchElementException(key))
  }

  def contains(key:String):Boolean = settings.containsKey(key)
}

private[spark] object SparkConf extends Logging{

}