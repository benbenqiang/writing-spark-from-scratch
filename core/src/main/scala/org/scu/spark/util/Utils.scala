package org.scu.spark.util

import java.io.{DataOutput, IOException, OutputStream, File}
import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID

import org.apache.commons.configuration.Configuration
import org.apache.commons.lang3.SystemUtils
import org.scu.spark.{Logging, SparkConf}

import scala.util.control.NonFatal
import scala.collection.JavaConverters._
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

  /**将ByteBuffer转化成byte数组，然后写入outputStream*/
  def writeByteBuffer(bb:ByteBuffer,out:OutputStream):Unit = {
    if(bb.hasArray){
      out.write(bb.array(),bb.arrayOffset()+bb.position(),bb.remaining())
    }else{
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }
  /**将ByteBuffer转化成byte数组，然后写入DataOutput*/
  def writeByteBuffer(bb:ByteBuffer,out:DataOutput):Unit = {
    if(bb.hasArray){
      out.write(bb.array(),bb.arrayOffset()+bb.position(),bb.remaining())
    }else{
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }

  /**获取系统属性*/
  def getSystemProperties:Map[String,String]={
    System.getProperties.stringPropertyNames().asScala.map(key=>(key,System.getProperty(key))).toMap
  }

  /**
   * 从URI中获取文件的名称
    **/
  def decodeFileNameInURI(uri:URI):String={
    val rawPath = uri.getRawPath
    val rawFileName = rawPath.split("/").last
    new URI("file:///"+rawFileName).getPath.substring(1)
  }

  /**将特定的文件或目录转移到sparkLocalDir中，供一个Executor的多个task使用。*/
  def fetchFile(
               url:String,
               targetDir:File,
               conf:SparkConf,
               hadoopConf:Configuration,
               timestamp:Long,
               useCache:Boolean): Unit ={
    val fileName = decodeFileNameInURI(new URI(url))
    val targetFile = new File(targetDir,fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache",defaultValue = true)
    if(useCache && fetchCacheEnabled){
      ???
    }else{
      ???
    }
  }
  /**获取装载spark的ClassLoader*/
  def getSparkClassLoader:ClassLoader = getClass.getClassLoader

  /**返回当前线程的ClassLoader或者返回装载spark的classLoader*/
  def getContextOrSparkClassLoader:ClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**根据类名获取该类的Class对象*/
  def classForName[T](className:String):Class[_]={
    Class.forName(className,true,getContextOrSparkClassLoader)
  }

  /**返回Task任务数据的最大值 1GB 以字节为单位*/
  def getMaxResultSize(conf:SparkConf):Long = {
    conf.get("spark.dirver.maxResultSize","1").toLong * 1024 *1024 * 1024
  }

  /**是否单机模式*/
  def isLocalMaster(conf:SparkConf):Boolean = {
    val master = conf.get("spark.master","local")
    master.startsWith("local")
  }
}
