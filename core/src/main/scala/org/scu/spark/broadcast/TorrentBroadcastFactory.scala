package org.scu.spark.broadcast

import org.scu.spark.SparkConf

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/5/5
 */
private[spark] class TorrentBroadcastFactory extends  BroadcastFactory{
  override def initialize(isDriver: Boolean, conf: SparkConf): Unit = {}

  override def stop(): Unit = {}

  /** *
    * 给定一个Broadcast的id，进行销销毁
    * @param blocking 是否阻塞调用
    */
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    TorrentBroadcast.unperist(id,removeFromDriver,blocking)
  }

  /**
   * 给定一个数据，创建相应类型的BroadCast
   * @param value_ 需要广播的数据
   * @param isLocal 是否用local 模式
   * @param id 广播变量的唯一标识
   */
  override def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long): Broadcast[T] = {
    new TorrentBroadcast[T](value_,id)
  }

}
