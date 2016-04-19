package org.scu.spark.util

import akka.util.Timeout
import org.scu.spark.SparkConf
import scala.concurrent.duration._
/**
 * Created by bbq on 2016/1/19
 */
private[spark] object RpcUtils {

  def askRpcTimeout(conf:SparkConf):Timeout={
    val durations = conf.getInt("spark.rpc.askTimeout",120)
    Timeout(durations seconds)
  }

  def maxMessageSizeBytes(conf:SparkConf):Int={
    val maxSizeInMB = conf.getInt("spark.rpc.message.maxSize",128)
    maxSizeInMB * 1024 *1024
  }
}
