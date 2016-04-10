package org.scu.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import org.scu.spark.Logging

import scala.collection.mutable
import scala.collection.mutable._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by  bbq on 2015/12/14
 */
private[spark] class TaskSetManager (
                                    sched:TaskSchedulerImpl,
                                    val taskSet:TaskSet,
                                    val maxTaskFailures:Int
                                      ) extends Schedulable with Logging{

  val conf = sched.sc.conf

  override var parent: Pool = _

  override def addSchedulable(schedulable: Schedulable): Unit = ???

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = ???

  //def schedulingMode:SchedulingMode
  override def weight: Int = ???

  val runningTaskSet = new HashSet()

  override def runingTasks: Int = runningTaskSet.size

  /**每个task根据preferedLocation，会指定相应的Executor，或者host，或者Rack，或者没有*/
  private val pendingTasksForExecutor = new HashMap[String,ArrayBuffer[Int]]

  private val pendingTasksForHost = new HashMap[String,ArrayBuffer[Int]]

  private val pendingTasksForRack = new HashMap[String,ArrayBuffer[Int]]

  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  val allPendingTasks = new ArrayBuffer[Int]

  var myLocalityLevels = computeValidLocalityLevels()

  override def checkSpeculatableTasks(): Boolean = ???

  override var name: String = "TaskSet_"+taskSet.stageId.toString

  override def removeSchedulable(schedulable: Schedulable): Unit = ???

  override def priority: Int = ???

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = ???

  override def minShare: Int = ???

  override def getSchedulableByName(name: String): Schedulable = ???

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = ???

  override def stageId: Int = ???

  /**获取不同locality所相应的延迟调度时间*/
  private def getLocalWait(level:TaskLocality.TaskLocaliry) : Long = {
    /**
     * 默认所有locatlity等级在未满足运行条件时，所等待的时间
     * 如果为0说明不会因为不满足locality而延迟执行
     * */
    val defaultWait = conf.getInt("spark.locality.wait",3000)
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }
    if(localityWaitKey != null){
      conf.getInt(localityWaitKey,defaultWait).toLong
    }else{
      0L
    }
  }

  /**
   * 计算在当前TaskManager中已经提交的task里，每个task有自己的preferedlocation
   * 根据本地性等级，返回所有已满足的本地性等级，从ProcessLocal开始优先判断是否需要延迟执行
   * 比如，是个task中，有一个分配满足ProcessLocal，其他的都是Any，那么先执行ProcessLocal
   * 其他的任务根据配置等待一段时间，然后再运行，此时之前被占用的资源可能会被释放
    * */
  private def computeValidLocalityLevels():Array[TaskLocality.TaskLocaliry]={
    import TaskLocality.{PROCESS_LOCAL,NODE_LOCAL,NO_PREF,RACK_LOCAL,ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocaliry]

    if(pendingTasksForExecutor.nonEmpty && getLocalWait(PROCESS_LOCAL) != 0 &&
      pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive)){
      levels += PROCESS_LOCAL
    }
    if(pendingTasksForHost.nonEmpty && getLocalWait(NODE_LOCAL) != 0 &&
    pendingTasksForHost.keySet.exists(sched.hasExecuotrsAliveOnHost)){
      levels += NODE_LOCAL
    }
    if(pendingTasksWithNoPrefs.nonEmpty)
      levels += NO_PREF
    if(pendingTasksForRack.nonEmpty && getLocalWait(RACK_LOCAL) != 0 &&
      pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack))
      levels += RACK_LOCAL
    levels += ANY

    logDebug("Valid locality levels for"+ taskSet +":"+ levels.mkString(","))
    levels.toArray
  }

  def executorAdded(): Unit ={
    ???
  }
}
