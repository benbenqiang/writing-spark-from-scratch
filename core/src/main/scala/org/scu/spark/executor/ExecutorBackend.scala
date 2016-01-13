package org.scu.spark.executor

import java.nio.ByteBuffer

import org.scu.spark.deploy.TaskState.TaskState

/**
 * Created by bbq on 2016/1/13
 */
private[spark] trait ExecutorBackend {
  def startsUpdate(taskId:Long,state:TaskState,data:ByteBuffer)
}
