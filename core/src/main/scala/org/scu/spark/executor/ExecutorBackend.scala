package org.scu.spark.executor

import java.nio.ByteBuffer

import org.scu.spark.TaskState.TaskState

/**
 * Created by bbq on 2016/1/13
 */
private[spark] trait ExecutorBackend {
  def statusUpdate(taskId:Long,state:TaskState,data:ByteBuffer)
}
