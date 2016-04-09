package org.scu.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import org.scu.spark.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by  bbq on 2015/12/14
 */
private[spark] class TaskSetManager (
                                    sched:TaskSchedulerImpl,
                                    val taskSet:TaskSet,
                                    val maxTaskFailures:Int
                                      ) extends Schedulable with Logging{
  override var parent: Pool = _

  override def addSchedulable(schedulable: Schedulable): Unit = ???

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = ???

  //def schedulingMode:SchedulingMode
  override def weight: Int = ???

  val runningTaskSet = new mutable.HashSet()

  override def runingTasks: Int = runningTaskSet.size

  override def checkSpeculatableTasks(): Boolean = ???

  override var name: String = "TaskSet_"+taskSet.stageId.toString

  override def removeSchedulable(schedulable: Schedulable): Unit = ???

  override def priority: Int = ???

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = ???

  override def minShare: Int = ???

  override def getSchedulableByName(name: String): Schedulable = ???

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = ???

  override def stageId: Int = ???

  def executorAdded(): Unit ={
    ???
  }
}
