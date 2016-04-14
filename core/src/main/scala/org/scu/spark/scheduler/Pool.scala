package org.scu.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.scu.spark.Logging
import org.scu.spark.scheduler.SchedulingMode.SchedulingMode

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by bbq on 2016/4/3
 */
private[spark] class Pool(
                           val poolName: String,
                           val schedulingMode: SchedulingMode,
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

  /**根据mode决定不同的调度算法，对taskset的执行顺序进行排序*/
  var taskSetSchedulingAlgoritm :SchedulingAlgorithm={
    schedulingMode match {
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm
      case SchedulingMode.FAIR =>
        new FIFOSchedulingAlgorithm
    }
  }


  override def addSchedulable(schedulable: Schedulable): Unit = ???

  override def checkSpeculatableTasks(): Boolean = ???

  override def removeSchedulable(schedulable: Schedulable): Unit = ???

  /**对调度器中的TaskSet进行排序*/
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulabQueue = schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgoritm.compare)
    for (schedulable <- sortedSchedulabQueue){
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  override def getSchedulableByName(name: String): Schedulable = ???

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = ???

  override def stageId: Int = ???

  def increaseRunningTask(taskNum:Int): Unit ={
    runningTasks += taskNum
    if(parent != null){
      parent.increaseRunningTask(taskNum)
    }
  }
}
