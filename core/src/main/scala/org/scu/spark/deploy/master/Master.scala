package org.scu.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, Props}
import org.scu.spark.deploy.ApplicationDescription
import org.scu.spark.deploy.DeployMessage._
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcAddress, RpcEnvConfig}
import org.scu.spark.{Logging, SparkConf}

import scala.collection.mutable

/**
 * spark集群的Master节点，主要职责：
 * 1.收取worker节点的心跳信息
 * 2.汇总集群资源情况
 * Created by bbq on 2015/11/10
 */
private[deploy] class Master(
                              akkaRpcEnv: AkkaRpcEnv,
                              address: RpcAddress,
                              val conf: SparkConf
                              ) extends Actor with Logging {
  val workers = new mutable.HashSet[WorkerInfo]()

  private val idToWorker = new mutable.HashMap[String, WorkerInfo]

  private val nextAppNumber = new AtomicInteger(0)
  val createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  private val masterUrl = address.toSparkURL

  /**应用占用最大的Cores个数，默认Int最大*/
  private val defaultCores = conf.getInt("spark.deploy.defaultCores",Int.MaxValue)

  override def preStart() = {
    logInfo("Starting Spark Master at" + masterUrl)
    logInfo(s"Running Spark version ${org.scu.spark.SPARK_VERSION}")
  }

  override def receive: Receive = {
    //收到Worker的注册信息
    case RegisterWorker(id, host, port, cores, memory) =>
      logInfo(s"Registering worker $id with $cores cores, $memory RAM")
      if(idToWorker.contains(id)){
        sender() ! RegisterWorkerFaild("Duplicate worker ID")
      }else{
        val workerInfo = new WorkerInfo(id, host, port, cores, memory, sender())

      }
//      idToWorker.put(id, workerInfo)
      sender() ! RegisteredWorker()

    case Heartbeat(workerId) =>
      idToWorker.get(workerId) match {
        case Some(workInfo) =>
          workInfo.lastHeartbeat = System.currentTimeMillis()
          logDebug(s"Receiving HeartBeet from $workerId")
        case None =>
          logError(s"Got heartbeat from unregistered worker $workerId")
      }
    case RegisterApplication(description) =>
      println("Registering app" + description.name)
      val app = createApplication(description,sender())
      logInfo("Registerd app"+description.name+ " with ID "+app.id)
      //TODO PersisteneEngine 用于容灾恢复
      sender() ! RegisteredApplication(app.id,self)
      schedule()
  }

  /**根据创建时间，driveer的actorRef，创建application信息*/
  private def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now,appId,desc,date,driver,defaultCores)
  }

  /**获取Application ID */
  private def newApplicationId(submitDate:Date):String = {
    "app-%s-%04d".format(createDateFormat.format(submitDate),nextAppNumber.getAndIncrement())
  }

  /**
   *  调度当前可用的资源。当新添加一个app或者资源变化时调用该方法
   */
  private def schedule():Unit={
    //TODO recoverStage judge

  }

  private def registerWorker(worker:WorkerInfo):Boolean={
    workers.filter{w=>
      (w.host == worker.host && w.port == worker.port)
    }
    ???
  }

}

object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ACTOR_NAME = "Master"

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val masterHost = conf.get("spark.master.host")
    val masterPort = conf.getInt("spark.master.port")
    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, masterHost, masterPort)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))

    val actorRef = rpcEnv.doCreateActor(Props(new Master(rpcEnv, RpcAddress(masterHost, masterPort), conf)), ACTOR_NAME)
  }
}