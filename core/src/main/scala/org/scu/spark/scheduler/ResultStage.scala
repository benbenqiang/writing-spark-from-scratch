package org.scu.spark.scheduler

import org.scu.spark.TaskContext
import org.scu.spark.rdd.RDD

/**
 * Created by bbq on 2015/11/23
 */
private[spark] class ResultStage(
                                id:Int,
                                rdd:RDD[_],
                                val func: (TaskContext,Iterator[_]) => _ ,
                                val partitions:Seq[Int],
                                parents:List[Stage],
                                firstJobId:Int
                                  ) extends Stage(id,rdd,partitions.length,parents,firstJobId){

  private[this] var _activeJob :Option[ActiveJob] = None

  def activeJob:Option[ActiveJob] = _activeJob

  def setActiveJob(job:ActiveJob)={
    _activeJob = Option(job)
  }

  def removeActiveJob()={
    _activeJob = None
  }

  /** 返回需要计算的partitionID */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage" + id
}
