package org.scu.spark.scheduler

import java.util.Properties

/**
 * 一组Task组成的Set，提交给TaskScheduler
 * Created by bbq on 2015/12/7
 */
private[spark] class TaskSet(
                            val tasks : Array[Task[_]],
                            val stageId : Int,
                            val stageAttemptId:Int,
                            val priority : Int,
                            val properties:Properties
                              ) {
  val id : String = stageId + "." + stageAttemptId

  override def toString : String = "TaskSet" + id
}
