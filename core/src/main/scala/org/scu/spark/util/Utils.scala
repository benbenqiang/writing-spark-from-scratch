package org.scu.spark.util

import org.scu.spark.SparkConf

/**
 * Created by bbq on 2015/12/23
 */
object Utils {
  def checkHostPort(hostPort:String,message:String=""): Unit ={
    assert(hostPort.indexOf(":") != -1,message)
  }
  /** 分割参数如：
    * -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
    * 目前只支持空格分隔！
    */
  def splitCommandString(s:String):Seq[String]={
    s.split(" ")
  }

  /**将conf中的配置转化成 -D 的java配置的形式*/
  def sparkJavaOpts(conf:SparkConf,filterKey:(String=>Boolean) = _ =>true):Seq[String]={
    conf.getAll
      .filter{case(k,_) =>filterKey(k)}
    .map{case (k,v) => s"-D$k=$v"}
  }



}
