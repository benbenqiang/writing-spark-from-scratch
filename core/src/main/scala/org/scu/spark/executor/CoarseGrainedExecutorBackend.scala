package org.scu.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import org.scu.spark.deploy.TaskState.TaskState
import org.scu.spark.rpc.akka.AkkaRpcEnv
import org.scu.spark.scheduler.cluster.CoarseGrainedClusterMessage
import org.scu.spark.scheduler.cluster.CoarseGrainedClusterMessage.{RegisterExecutorResponse, RegisterExecutor, KillTask}
import org.scu.spark.util.ThreadUtils
import org.scu.spark.{Logging, SparkEnv}

import scala.concurrent.Future

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
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap{ ref => {
      driver = Some(ref)
      val a= (ref ? RegisterExecutor(executorId,self,hostPort,cores,extractLogUrls)).mapTo[RegisterExecutorResponse]
      a
    }}
  }

  def extractLogUrls:Map[String,String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
    .map(e=> (e._1.substring(prefix.length).toLowerCase,e._2))
  }

  override def receive: Receive = ???

  override def startsUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = ???
}