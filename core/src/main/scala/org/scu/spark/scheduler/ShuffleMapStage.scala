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
  private[this] var numAvailableOutputs_ : Int = 0

  /**
   * 每一个List[MapStatus]代表每一个partition的运行状态，之所以是List是因为每一个partition可能
   * 运行多次
   */
  private[this] val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)

  def isAvailable = numAvailableOutputs_ == numPartitions

  /** 返回需要计算的partitionID */
  override def findMissingPartitions(): Unit = {
    val missing = (0 until numPartitions).filter(outputLocs(_).isEmpty)
    assert(missing.size == numPartitions - numAvailableOutputs_,
    s"${missing.size} missing,expected ${numPartitions - numAvailableOutputs_}")
    missing
  }
}
