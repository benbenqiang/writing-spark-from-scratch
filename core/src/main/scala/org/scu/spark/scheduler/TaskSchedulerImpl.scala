package org.scu.spark.scheduler

import org.scu.spark.executor.TaskMetrics
import org.scu.spark.storage.BlockManagerID
import org.scu.spark.{Logging, SparkContext}

/**
 * 通过SchedulerBackend在不同的集群上调度task
 *
 * Created by bbq on 2015/12/11
 */
private[spark] class TaskSchedulerImpl(
                                      val sc:SparkContext,
                                      val maxTaskFailures:Int,
                                      isLocal:Boolean = false = 4
                                        ) extends TaskScheduler with Logging{
  def this(sc:SparkContext) = this(sc)

  override def start(): Unit = ???

  override def applicationAttemptId(): Option[String] = ???

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def canelTasks(stageId: Int, interruptThread: Boolean): Unit = ???

  override def submitTasks(taskSet: TaskSet): Unit = ???

  override def executorLost(executorId: String, reason: String): Unit = ???

  override def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)], blockManagerID: BlockManagerID): Boolean = ???

  override def setDAGScheduler(dAGScheduler: DAGScheduler): Unit = ???
}
