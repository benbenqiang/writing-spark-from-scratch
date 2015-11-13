package org.scu.spark.deploy.master

import akka.actor.{Actor, Props}
import org.scu.spark.Logging
import org.scu.spark.deploy.DeployMessage._
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcEnvConfig}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * spark集群的Master节点，主要职责：
 * 1.收取worker节点的心跳信息
 * 2.汇总集群资源情况
 * Created by bbq on 2015/11/10
 */
class Master extends Actor with Logging {

  private val idToWorker = new mutable.HashMap[String, WorkerInfo]

  override def preStart() = {
    logInfo("Started Master Actor")
  }

  override def receive: Receive = {
    //收到Worker的注册信息
    case RegisterWorker(id, host, port, cores, memory) =>
      logInfo(s"Registering worker $id with $cores cores, $memory RAM")
      val workerInfo = new WorkerInfo(id, host, port, cores, memory, sender())
      idToWorker.put(id, workerInfo)
      sender() ! RegisteredWorker()

    case Heartbeat(workerId)=>
      logDebug("Receive HeartBeat")
      idToWorker.get(workerId) match {
        case Some(workInfo) =>
          workInfo.lastHeartbeat = System.currentTimeMillis()
          logInfo(s"Receving from $workerId")
      }
  }
}

object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ACTOR_NAME = "Master"

  def main(args: Array[String]) {
    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, "127.0.0.1", 60005)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))
    val actorRef = rpcEnv.doCreateActor(Props(classOf[Master]), ACTOR_NAME)
  }
}