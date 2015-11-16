package org.scu.spark

/**
 * Created by bbq on 2015/11/16
 */
abstract class Partitioner extends Serializable{
  def numPartitions:Int
  def getPartition(key:Any):Int
}
