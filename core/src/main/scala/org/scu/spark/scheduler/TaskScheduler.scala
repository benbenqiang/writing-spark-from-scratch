package org.scu.spark.scheduler

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

//  def submitTasks(taskSe:TaskSet)

}
