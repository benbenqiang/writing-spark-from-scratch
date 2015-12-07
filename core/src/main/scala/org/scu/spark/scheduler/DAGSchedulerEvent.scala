package org.scu.spark.scheduler

import java.util.Properties

import org.scu.spark.TaskContext
import org.scu.spark.rdd.RDD

/**
 * DAGShceduler所可以处理的信息。
 * Created by bbq on 2015/11/20
 */
private[scheduler] sealed trait DAGSchedulerEvent

private case class JobSubmit(
                            jobId:Int,
                            finalRDD:RDD[_],
                            func:(TaskContext,Iterator[_]) => _,
                            partitions:Seq[Int],
                            listener:JobListerner,
                            properties:Properties
                              ) extends DAGSchedulerEvent
