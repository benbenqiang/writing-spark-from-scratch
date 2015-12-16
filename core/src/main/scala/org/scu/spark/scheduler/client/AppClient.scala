package org.scu.spark.scheduler.client

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Future, ScheduledFuture}

import akka.actor.Actor
import akka.actor.Actor.Receive
import org.scu.spark.deploy.DeployMessage.RegisterApplication
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
                                listener: AppClientListener,
                                conf: SparkConf
                                ) extends Logging {

  private val masterRpcAddresses = masterUrls

  private val registered = new AtomicBoolean(false)

  private class ClientEndPoint(val rpcEnv: AkkaRpcEnv) extends Actor with Logging {
    /** Master的RPC远程对象 */
    private var master: Option[Actor] = None

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
          masterRef ! RegisterApplication
        } catch {
          case ie: InterruptedException =>
          //TODO notfatal
        }
      })
    }

    private def registerWithMaster()={
      registerMasterFutures.set(tryRegisterAllMasters())
    }

    override def receive: Receive = ???
  }

  def start(): Unit = {

  }
}
object AppClient{
  def main(args: Array[String]) {

  }
}