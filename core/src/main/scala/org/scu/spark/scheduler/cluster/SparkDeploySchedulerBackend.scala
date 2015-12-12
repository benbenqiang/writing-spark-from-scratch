package org.scu.spark.scheduler.cluster

import org.scu.spark.{Logging, SparkContext}
import org.scu.spark.scheduler.TaskSchedulerImpl

/**
 * Spark Standalone 用于分配资源。
 * Created by bbq on 2015/12/11
 */
class SparkDeploySchedulerBackend (
                                  scheduler:TaskSchedulerImpl,
                                  sc:SparkContext,
                                  masters:String
                                    )
extends CoarseGrainedSchedulerBackend(scheduler,sc.env.rpcEnv)
//TODO AppClientListener
with Logging
{


}
