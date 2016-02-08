package org.scu.spark.util

import org.apache.commons.lang3.SystemUtils
import org.scu.spark.{Logging, SparkConf}

/**
 * Created by bbq on 2015/12/23
 */
private[spark] object Utils extends Logging{
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

  /**判断底层系统*/
  val isWindows = SystemUtils.IS_OS_WINDOWS
  val isMac = SystemUtils.IS_OS_MAC

  /**返回当前系统的*/
  def libraryPathName : String = {
    if(isWindows){
      "PATH"
    } else if(isMac){
      "DYLD_LIBRARY_PATH"
    } else {
      "LD_LIBRARY_PATH"
    }
  }



}
