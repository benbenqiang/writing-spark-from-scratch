package org.scu.spark.scheduler.cluster

import org.scu.spark.rpc.akka.AkkaRpcEnv
import org.scu.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl}

/**
 * 一个粗粒度的调度器，为一个job分配固定的资源，为多个task共享，
 * 而不是运行完一个归还资源，并且为新Task重新分配资源。
 * Created by bbq on 2015/12/11
 */
private[spark] class CoarseGrainedSchedulerBackend(scheduler:TaskSchedulerImpl,val rpcEnv:AkkaRpcEnv)
extends SchedulerBackend{
  override def start(): Unit = ???

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def reviveOffers(): Unit = ???
}
