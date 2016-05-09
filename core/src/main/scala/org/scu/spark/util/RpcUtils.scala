package org.scu.spark.util

import akka.actor.ActorRef
import akka.util.Timeout
import org.scu.spark.{Logging, SparkEnv, SparkConf}
import org.scu.spark.rpc.akka.{RpcAddress, AkkaRpcEnv}
import scala.concurrent.duration._
/**
 * Created by bbq on 2016/1/19
 */
private[spark] object RpcUtils extends Logging{

  def makeDriverRef(name:String,conf:SparkConf,rpcEnv:AkkaRpcEnv):ActorRef={
    val driverActorASystemName = SparkEnv.driverActorSystemName
    val driverHost = conf.get("spark.driver.host","127.0.0.1")
    val driverPort = conf.getInt("spark.driver.port",7077)
    logInfo("Connecting to driver actor : $name ")
    rpcEnv.setupEndpointRef(driverActorASystemName,RpcAddress(driverHost,driverPort),name)
  }

  def askRpcTimeout(conf:SparkConf):Timeout={
    val durations = conf.getInt("spark.rpc.askTimeout",120)
    Timeout(durations seconds)
  }

  def maxMessageSizeBytes(conf:SparkConf):Int={
    val maxSizeInMB = conf.getInt("spark.rpc.message.maxSize",128)
    maxSizeInMB * 1024 *1024
  }
}
