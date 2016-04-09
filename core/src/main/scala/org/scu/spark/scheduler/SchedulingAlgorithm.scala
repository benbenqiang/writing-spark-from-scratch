package org.scu.spark.scheduler

/**
 * TaskManagers 之间的调度，比较两个Schedulable的执行顺序
 * Created by bbq on 2016/4/9
 */
private[spark] trait SchedulingAlgorithm {
  def compare(s1: Schedulable, s2: Schedulable): Boolean
}

private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  /**
   * 首先通过jobid进行比较，jobid越大优先级越高
   * 若jobid相同则比较stageId
   **/
  override def compare(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      res = math.signum(s1.stageId - s2.stageId)
    }
    if (res < 0)
      true
    else
      false
  }
}
