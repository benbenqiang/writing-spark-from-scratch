package org.scu.spark.scheduler

import org.scu.spark.{Accumulator, Logging, Partition}

/**
 * Shuffle Map端的Tas
 * Created by bbq on 2015/12/9
 */
private[spark] class ShuffleMapTask(
                                     stageId: Int,
                                     stageAttemptId: Int,
                                     //TODO taskBinary
                                     partition: Partition,
                                     @transient private var _locs: Seq[TaskLocation],
                                     internalAccumulators: Seq[Accumulator[Long]]
                                     ) extends Task[MapStatus](stageId, stageAttemptId, partition.index, internalAccumulators)
with Logging {

}
