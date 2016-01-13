package org.scu.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef}
import org.scu.spark.deploy.TaskState.TaskState
import org.scu.spark.rpc.akka.AkkaRpcEnv
import org.scu.spark.{Logging, SparkEnv}

/**
 * Created by bbq on 2016/1/13
 */
private[spark] class CoarseGrainedExecutorBackend(
                                                   val rpcEnv: AkkaRpcEnv,
                                                   driverUrl: String,
                                                   executorId: String,
                                                   hostPort: String,
                                                   cores: Int,
                                                   userClassPath: Seq[URL],
                                                   env: SparkEnv
                                                   ) extends Actor with ExecutorBackend with Logging {
  var executor: Executor = null
  @volatile var driver: Option[ActorRef] = None


  override def preStart(): Unit = {
    logInfo("Connecting to driver:"+ driverUrl)
//    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap{ ref => {
//      driver = Some(ref)
//      ref ? RegisterExecutor()
//    }}
  }

  override def receive: Receive = ???

  override def startsUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = ???
}