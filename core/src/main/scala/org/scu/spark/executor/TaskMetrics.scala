package org.scu.spark.executor

/**
 * 用于跟踪executor的执行情况
 *
 *
 * Created by bbq 2015/12/11
 */
class TaskMetrics extends Serializable{
  private var _hostname : String = _
  def hostname:String = _hostname
}
