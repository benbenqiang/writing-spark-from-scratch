package org.scu.spark.broadcast

import org.scu.spark.SparkConf

import scala.reflect.ClassTag

/**
 * 实例化 Broadcast的工厂
 * Created by bbq on 2016/5/5.
 */
private[spark] trait BroadcastFactory {
  def initialize(isDriver:Boolean,conf:SparkConf):Unit

  /**
   * 给定一个数据，创建相应类型的BroadCast
   * @param value 需要广播的数据
   * @param isLocal 是否用local 模式
   * @param id 广播变量的唯一标识
   */
  def newBroadcast[T:ClassTag](value:T,isLocal:Boolean,id:Long):Broadcast[T]

  /** *
    * 给定一个Broadcast的id，进行销销毁
    * @param blocking 是否阻塞调用
    */
  def unbroadcast(id:Long,removeFromDriver:Boolean,blocking:Boolean):Unit

  def stop():Unit
}
