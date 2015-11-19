package org.scu.spark

/**
 * 包含决定数据如何分发到不同节点的散列函数
 * Created by bbq on 2015/11/16
 */
abstract class Partitioner extends Serializable{
  def numPartitions:Int
  def getPartition(key:Any):Int
}
