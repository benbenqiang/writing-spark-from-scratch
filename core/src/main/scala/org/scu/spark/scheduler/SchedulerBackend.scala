package org.scu.spark.scheduler

/**
 * 可插拔的资源调度类，属于一个TaskScheduler中，可以向特定资源管理器申请资源(Standalone,yarn,mesos)。
 * Created by bbq on 2015/12/11
 */
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis()

  def start(): Unit

  def stop(): Unit

  def reviveOffers(): Unit

  def defaultParallelism(): Int

  def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit =
    throw new UnsupportedOperationException

  def isReady:Boolean= true

  def applicationId():String = appId

  def applicationAttemptId():Option[String]=None


}
