package org.scu.spark.scheduler.client

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Future, ScheduledFuture}

import akka.actor.{Actor, ActorRef, Props}
import org.scu.spark.deploy.ApplicationDescription
import org.scu.spark.deploy.DeployMessage.{ExecutorAdded, RegisteredApplication, RegisterApplication}
import org.scu.spark.deploy.master.Master
import org.scu.spark.rpc.akka.{AkkaRpcEnv, RpcAddress}
import org.scu.spark.util.ThreadUtils
import org.scu.spark.{Logging, SparkConf}

/**
 * spark client向spark部署集群通讯的类。
 * Created by  bbq on 2015/12/15
 */
private[spark] class AppClient(
                                rpcEnv: AkkaRpcEnv,
                                masterUrls: RpcAddress,
                                appDescription: ApplicationDescription,
                                listener: AppClientListener,
                                conf: SparkConf
                                ) extends Logging {

  private val masterRpcAddresses = masterUrls

  private val registered = new AtomicBoolean(false)
  private val appId = new AtomicReference[String]
  private val clientEndPoint = new AtomicReference[ActorRef]

  private class ClientEndPoint(val rpcEnv: AkkaRpcEnv) extends Actor with Logging {
    /** Master的RPC远程对象 */
    private var master: Option[ActorRef] = None

    private var alreadyDisconnected = false
    private val alreadDead = new AtomicBoolean(false)
    private val registerMasterFutures = new AtomicReference[Future[_]]
    private val registrationRetryTImer = new AtomicReference[ScheduledFuture[_]]()

    /** 向master注册的线程池 */
    private val registerMasterThreadPool = ThreadUtils.newDeamonCachedThreadPool("applient-register-master-threadpool", 1)
    /** 向master重连的线程池 */
    private val registrationRetryThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    override def preStart(): Unit = {
      try {
        tryRegisterAllMasters()
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
      }
    }

    private def tryRegisterAllMasters(): Future[_] = {
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = try {
          if (registered.get) {
            return
          }
          logInfo("Connecting to master " + masterRpcAddresses + "...")
          val masterRef = rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterRpcAddresses, Master.ACTOR_NAME)
          masterRef ! RegisterApplication(appDescription)
        } catch {
          case ie: InterruptedException =>
          //TODO notfatal
        }
      })
    }

    private def registerWithMaster() = {
      registerMasterFutures.set(tryRegisterAllMasters())
    }

    override def receive: Receive = {
      /**向master注册application成功*/
      case RegisteredApplication(appId_,masterRef) =>
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        listener.connected(appId.get)

      case ExecutorAdded(id,workerId,hostPort,cores,memory) =>
        val fullId = appId + "/" + id
        logInfo(s"Executor added: $fullId on $workerId ($hostPort) with cores:$cores memory:$memory")
        listener.executorAdded(fullId,workerId,hostPort,cores,memory)
    }
  }

  def start(): Unit = {
    clientEndPoint.set(rpcEnv.doCreateActor(Props(new ClientEndPoint(rpcEnv)), "AppClient"))
  }
}

object AppClient {
  def main(args: Array[String]) {

  }
}