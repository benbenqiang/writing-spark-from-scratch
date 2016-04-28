package org.scu.spark.deploy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.scu.spark.{SparkConf, Logging}

/**
 * 与hadoop进行交互的类
 * Created by bbq on 2016/4/28
 */
class SparkHadoopUtil extends Logging{
  private val sparkConf = new SparkConf()
  val conf  = newConfiguration(sparkConf)
  UserGroupInformation.setConfiguration(conf)


  def newConfiguration(conf:SparkConf):Configuration = {
    val hadoopConf = new Configuration()

    if(conf != null){
      conf.getAll.foreach{case (key,value) =>
          if(key.startsWith("spark.hadoop.")){
            hadoopConf.set(key.substring("spark.hadoop.".length),value)
          }
      }
      val bufferSize = conf.get("spark.buffer.size","65536")
      hadoopConf.set("io.file.buffer.size",bufferSize)
    }
    hadoopConf
  }
}

object SparkHadoopUtil {
  private lazy val hadoop = new SparkHadoopUtil
  def get:SparkHadoopUtil=hadoop
}