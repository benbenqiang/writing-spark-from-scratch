package org.scu.spark.scheduler

import org.scu.spark.ShuffleDependency
import org.scu.spark.rdd.RDD

/**
 * 作为DAG的中间stage，有shuffle
 * Created by bbq on 2015/11/22
 */
class ShuffleMapStage(
                     id:Int,
                     rdd:RDD[_],
                     numTasks:Int,
                     parents:List[Stage],
                     firstJobId:Int,
                     val shuffleDep: ShuffleDependency[_,_,_]
                       )extends Stage(id,rdd,numTasks,parents,firstJobId){
  private[this] var _numAvailableOutputs:Int= 0

  def isAvailable = _numAvailableOutputs == numPartition
  /** 返回需要计算的partitionID */
  override def findMissingPartitions(): Unit = {
  }
}
