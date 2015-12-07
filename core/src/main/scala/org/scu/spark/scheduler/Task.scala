package org.scu.spark.scheduler

import org.scu.spark.Accumulator

/**
 * Spark中执行单元,有两种
 *
 * 1.scheduler.ShuffleMapTask
 * 2.scheduler.ResultTask
 *
 * 一个SparkJob分为一个或者多个stage。最后一个包含很多ResultTask，之前的stage由ShuffleMapTask组成。
 * Created by bbq on 2015/12/7
 */
private[spark] abstract class Task[T](
                                     val stageId : Int,
                                     val stageAttemptId:Int,
                                     val partitionId:Int,
                                     internalAccumulators:Seq[Accumulator[Long]]
                                       ) extends Serializable{
}
