package org.scu.spark.scheduler

/**
 * jobWaiter的状态
 * Created by bbq on 2015/11/19
 */
sealed trait JobResult

case object JobSucceeded extends JobResult

private case class JobFailed(exception: Exception) extends JobResult