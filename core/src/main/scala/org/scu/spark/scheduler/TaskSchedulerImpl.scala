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
                                      isLocal:Boolean = false
                                        ) extends TaskScheduler with Logging{

  def this(sc:SparkContext) = this(sc,sc.conf.getInt("spark.task.maxFailures",4))

  var _backend : SchedulerBackend = _

  def initialize(backend:SchedulerBackend) ={
    _backend = backend
    //TODO SchedulerBuilder
  }
  override def start(): Unit = {
    _backend.start()
  }

  override def applicationAttemptId(): Option[String] = ???

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def canelTasks(stageId: Int, interruptThread: Boolean): Unit = ???

  override def submitTasks(taskSet: TaskSet) = {
    val tasks = taskSet.tasks
    logInfo("Adding task set "+taskSet.id + " with " + tasks.length + "tasks")
    this.synchronized{
//      val manager = createTaskSet
    }
  }

  private[scheduler] def createTaskSetManager(
                                             taskSet: TaskSet,
                                             maxTaskFailures:Int
                                               ):TaskSetManager={
    new TaskSetManager(this,taskSet,maxTaskFailures)
  }
  override def executorLost(executorId: String, reason: String): Unit = ???

  override def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)], blockManagerID: BlockManagerID): Boolean = ???

  override def setDAGScheduler(dAGScheduler: DAGScheduler): Unit = ???
}
