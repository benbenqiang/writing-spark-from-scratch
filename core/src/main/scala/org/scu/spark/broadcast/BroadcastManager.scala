package org.scu.spark.broadcast


import java.util.concurrent.atomic.AtomicLong

import org.scu.spark.{Logging, SparkConf}

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/5/5
 */
private[spark] class BroadcastManager(
                                     isDriver:Boolean,
                                     conf:SparkConf
                                       ) extends Logging{
  private var initialized = false
  private var broadcastFactory : BroadcastFactory = null

  initialize()

  /**初始化BroadcastFactory,现在只有TorrentBroadcast*/
  private def initialize(): Unit ={
    synchronized{
      if(!initialized){
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver,conf)
        initialized = true
      }
    }
  }


  def stop(): Unit = {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  def newBroadcast[T:ClassTag](value_ : T, isLoal : Boolean) :Broadcast[T] ={
    broadcastFactory.newBroadcast[T](value_,isLoal,nextBroadcastId.getAndIncrement())
  }

}
