package org.scu.spark.scheduler

/**
 * 提交任务给DAGScheduler后，监听提交的结果，采用等待阻塞的方式
 * 每当一个task成功或失败，都会唤醒一次JobWaiter
 * Created by bbq on 2015/11/19
 */
private[scheduler] trait JobListerner {
  def taskSucceeded(index:Int,result:Any)
  def jobFailed(exception:Exception)
}
