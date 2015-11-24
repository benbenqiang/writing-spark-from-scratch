package org.scu.spark.scheduler


/**
 * 在DAGScheduler中运行的job，记录任务的运行情况
 * Created by bbq on 2015/11/24
 */
private[spark] class ActiveJob(
                              val jobId:Int,
                              val finalStage:Stage,
                              val listener:JobListerner
                                ) {
  val numPartitions = finalStage match{
      /** resultStage不需要计算所有的partitions*/
    case r : ResultStage =>r.partitions.length
    case m =>m.rdd.partitions.length
  }

  val finished = Array.fill[Boolean](numPartitions)(false)

  var numFinished = 0
}
