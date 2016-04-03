package org.scu.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.scu.spark.Logging

import scala.collection.mutable.ArrayBuffer

/**
 * Created by bbq on 2016/4/3
 */
private[spark] class Pool(
                           val poolName: String,
                           //TODO SchedulingMode
                           initMinShare: Int,
                           initWeight: Int
                           )
  extends Schedulable
  with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  var runningTasks = 0
  var priority = 0

  //var stageId = -1
  var name = poolName
  var parent:Pool = null

  override def runingTasks: Int = ???

  override def addSchedulable(schedulable: Schedulable): Unit = ???

  override def checkSpeculatableTasks(): Boolean = ???

  override def removeSchedulable(schedulable: Schedulable): Unit = ???

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = ???

  override def getSchedulableByName(name: String): Schedulable = ???

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = ???

  override def stageId: Int = ???
}
