package org.scu.spark.scheduler

import org.scu.spark.executor.TaskMetrics
import org.scu.spark.storage.BlockManagerID

/**
 * 可拔插式的Task Schedulers，从DAGScheduler获取一系列Tasks，并且将他们交给cluster执行
 * 处理stragglers。处理失败，并且想DAGScheduler传递信息。
 * Created by bbq on 2015/12/7
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application" + System.currentTimeMillis()

  //TODO: rootPool
  //TODO: schedulingMode

  def start():Unit

  //TODO: postStartHook

  def stop():Unit

  def submitTasks(taskSet:TaskSet):Unit

  def canelTasks(stageId:Int,interruptThread:Boolean)

  def setDAGScheduler(dAGScheduler: DAGScheduler):Unit

  def defaultParallelism() :Int

  /**
   * 更新executor传播来的metrics，并且查看是否blockManager是否存活。
   * driver如果返回true，说明blockmanager已注册，如果false，则需要re-register
   */
  def executorHeartbeatReceived(execId:String,taskMetrics:Array[(Long,TaskMetrics)],blockManagerID: BlockManagerID):Boolean

  def applicationId():String = appId

  def executorLost(executorId:String,reason:String):Unit

  def applicationAttemptId():Option[String]



}
