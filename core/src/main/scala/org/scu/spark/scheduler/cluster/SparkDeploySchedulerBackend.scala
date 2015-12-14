package org.scu.spark.scheduler.cluster

import org.scu.spark.rpc.akka.{RpcAddress, AkkaUtil}
import org.scu.spark.{SparkEnv, Logging, SparkContext}
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

  super.start()
  //TODO launcherBackend

  /**executor 远程连接Driver 的地址*/
  val driverUrl = AkkaUtil.address(
    SparkEnv.driverActorSystemName,sc.conf.get("spark.driver.host"),
    sc.conf.getInt("spark.driver.port"),CoarseGrainedSchedulerBackend.ENDPOINT_NAME)





}
