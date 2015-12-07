package org.scu.spark.scheduler

import java.util.Properties


/**
 * 在DAGScheduler中运行的job，分为两种类型。
 * 1.执行一个action。
 * 2.map 任务，计算map的输出，用于自适应性的查询计划，在下游任务执行之前做一些统计。
 *
 * 任务之间会共享之前的stage。
 *
 * @param jobId job的唯一id
 * @param finalStage 最后一个stage
 * @param listener jobWaiter 用于反馈任务的执行情况
 *                 Created by bbq on 2015/11/24
 */
private[spark] class ActiveJob(
                                val jobId: Int,
                                val finalStage: Stage,
                                val listener: JobListerner,
                                val properties: Properties
                                ) {
  val numPartitions = finalStage match {
    /** resultStage不需要计算所有的partitions */
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.rdd.partitions.length
  }

  val finished = Array.fill[Boolean](numPartitions)(false)

  var numFinished = 0
}
