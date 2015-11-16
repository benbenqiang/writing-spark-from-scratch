package org.scu.spark

/**
 * 记录每个RDD的partition信息
 * Created by bbq on 2015/11/16
 */
abstract class Partition extends Serializable{
  def index:Int

  override def hashCode(): Int = index
}
