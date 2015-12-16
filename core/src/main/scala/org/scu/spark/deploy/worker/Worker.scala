package org.scu.spark.deploy.worker

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{TimeUnit, Executors}

import akka.actor.{PoisonPill, Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import org.scu.spark.Logging
import org.scu.spark.deploy.DeployMessage._
import org.scu.spark.deploy.master.Master
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcAddress, RpcEnvConfig}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Created by bbq on 2015/11/11
 */
class Worker(
              rpcEnv: AkkaRpcEnv,
              masterRpcAddress: RpcAddress,
              cores: Int,
              memory: Int
              ) extends Actor with Logging {
  //用于生成Woker的ID
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port
  private var master: Option[ActorRef] = None
  private val workerId = generateWorkerId()
  private val HEARTBEAT_MILLIS= 30 * 1000

  override def preStart() = {
    logInfo("Starting Spark worker")
    //向Master注册
    tryRegisterMasters()
  }

  override def receive: Receive = {
    case SendHeartbeat =>
      logDebug("Send hearbeat to master")
      master.get ! Heartbeat(workerId)
    case _ =>
      logError("no receive defined!")
  }

  /**
   * 链接远程Master的Actor
   */
  def tryRegisterMasters() = {
    logInfo("Connecting to master:" + masterRpcAddress)
    val masterEndpoint = rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterRpcAddress, Master.ACTOR_NAME)
    registerMaster(masterEndpoint)
  }

  /**
   * 链接成功后向Master注册Worker
   */
  def registerMaster(master: ActorRef) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = Timeout(5 seconds)
    val future = master ? RegisterWorker(workerId, host, port, cores, memory)
    //异步回调
    future.onComplete{
      case Success(response)=>
        handleRegisterResponse(response,master)
      case Failure(response)=>
        logError("Register to Master Error :"+response)
        System.exit(1)
    }

  }


  /**
   * 处理向Master注册后，Master返回的信息
   */
  def handleRegisterResponse(msg: Any,master:ActorRef) = {
    msg match {
      case RegisteredWorker() =>
        changeMaster(master)
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable {
          override def run(): Unit = self ! SendHeartbeat
        },0,HEARTBEAT_MILLIS,TimeUnit.MILLISECONDS)
      case RegisterWorkerFaild(str) =>
        logError("Worker registration failed: " + str)
    }
  }

  /**
   * 配置Worker的MasterActorRef
   */
  def changeMaster(masterRef: ActorRef) = {
    master = Some(masterRef)
  }

  /**
   * 根据时间和host：port 生成WokerID
   * @return
   */
  private def generateWorkerId(): String = {
    val date = createDateFormat.format(new Date)
    s"Worker-$date-$host-$port"
  }
}

object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ACTOR_NAME = "Worker"

  def main(args: Array[String]) {
    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, "127.0.0.1", 60001)
    val masterRpcAddress = RpcAddress("127.0.0.1", 60000)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))
    val actorRef = rpcEnv.doCreateActor(Props(classOf[Worker], rpcEnv, masterRpcAddress, 2, 1024), ACTOR_NAME)
  }
}