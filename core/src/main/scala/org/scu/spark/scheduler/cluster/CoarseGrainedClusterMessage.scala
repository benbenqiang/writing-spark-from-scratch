package org.scu.spark.scheduler.cluster

import java.nio.ByteBuffer

import akka.actor.ActorRef
import org.scu.spark.deploy.TaskState.TaskState

/**
 * Created by bbq on 2016/1/15
 */

/** */
private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessage {

  /** Driver节点发给Executor的消息 */
  case class LaunchTask(data: ByteBuffer) extends CoarseGrainedClusterMessage

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean) extends CoarseGrainedClusterMessage

  sealed trait RegisterExecutorResponse

  case class RegisteredExectutor(hostname: String) extends CoarseGrainedClusterMessage with RegisterExecutorResponse

  case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessage with RegisterExecutorResponse

  /** Executor发送给dirver的信息 */
  case class RegisterExecutor(
                               executorId: String,
                               executorRef: ActorRef,
                               hostPort: String,
                               cores: Int,
                               logUrls: Map[String, String]
                               ) extends CoarseGrainedClusterMessage

  case class StatusUpdate(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer) extends CoarseGrainedClusterMessage

  case object RetrieveSparkProps extends CoarseGrainedClusterMessage
}
