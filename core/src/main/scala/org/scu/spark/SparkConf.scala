package org.scu.spark

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Created by bbq on 2015/12/11
 */
class SparkConf extends Cloneable with Logging {
  private val settings = new ConcurrentHashMap[String, String]()

  set("spark.driver.host", "127.0.0.1")
  set("spark.driver.port", "60010")
  set("spark.master.host", "127.0.0.1")
  set("spark.master.port", "60000")
  set("spark.app.name", "defaultAppName")
  set("spark.executor.memory", "1024")
  set("spark.executor.cores","2")
  set("spark.executor.port","7655")
  set("spark.scheduler.mode","FIFO")

  def set(key: String, value: String): SparkConf = {
    if (key == null || value == null) {
      throw new NullPointerException(s"null value for key or value,key=$key value=$value")
    }
    settings.put(key, value)
    this
  }

  def setAll(settings: Traversable[(String, String)]): SparkConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getInt(key: String): Int = {
    getOption(key).map(_.toInt).getOrElse(throw new NoSuchElementException(key))
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getLong(key: String): Long = {
    getOption(key).map(_.toLong).getOrElse(throw new NoSuchElementException(key))
  }

  def getBoolean(key:String,defaultValue:Boolean) : Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map { ent => (ent.getKey, ent.getValue) }.toArray
  }

  override def clone: SparkConf = {
    new SparkConf().setAll(getAll)
  }

  def contains(key: String): Boolean = settings.containsKey(key)
}

private[spark] object SparkConf extends Logging {
  /**
   * 过滤需要传给executor 启动的参数
   */
  def isExecutorStartupConf(name: String): Boolean = {
    name.startsWith("akka") ||
      name.startsWith("spark.akka") ||
      name.startsWith("spark.auth") ||
      name.startsWith("spark.ssl") ||
      name.startsWith("spark.rpc") ||
      isSparkPortConf(name)
  }

  /** 是否是端口配置参数 */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") || name.endsWith(".port")) || name.startsWith("spark.port.")
  }
}
