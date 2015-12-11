package org.scu.spark.scheduler.cluster

import org.scu.spark.SparkContext
import org.scu.spark.scheduler.TaskSchedulerImpl

/**
 * Spark Standalone 用于分配资源。
 * Created by bbq on 2015/12/11
 */
class SparkDeploySchedulerBackend (
                                  scheduler:TaskSchedulerImpl,
                                  sc:SparkContext,
                                  masters:Array[String]
                                    ){

}
