package org.scu.spark.util

import java.io.File
import java.util.UUID

import org.apache.commons.lang3.SystemUtils
import org.scu.spark.{Logging, SparkConf}

import scala.util.control.NonFatal

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

  /**递归的创建目录*/
  def createDirectory(root:String,namePrefix:String = "spark"):File={
    var dir = new File( root,namePrefix + "-" + UUID.randomUUID().toString )
    if (dir.exists() || !dir.mkdirs())
      dir = null
    dir.getCanonicalFile
  }

  def createTempDir(
                   root:String=System.getProperty("java.io.tmpdir"),
                   namePrefix:String="spark"):File={
    val dir = createDirectory(root,namePrefix)
    //TODO JVM down后自动删除文件
    dir
  }

  /**使用call-by-name
    * 仅仅抛出fatal的异常
    * */
  def tryLogNonFatalError(block: => Unit)={
    try{
      block
    }catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}",t)
    }
  }

}
