package org.scu.spark.scheduler

/**
 * 在TaskSet中来表示一个运行的任务
 * Created by bbq on 2016/4/11
 */
class TaskInfo (
               val taskId:Long,
               val index:Int,
               val attemptNumber:Int,
               val launchTime:Long,
               val executorId:String,
               val host:String,
               val taskLocality: TaskLocality.TaskLocality,
               val speculative:Boolean
                 ){
  def id:String = s"$index.$attemptNumber"
}
