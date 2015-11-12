package org.scu.spark.deploy.master

import akka.actor.{Props, Actor}
import org.scu.spark.Logging
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcEnvConfig}
import org.scu.spark.deploy.DeployMessage._

/**
 * spark集群的Master节点，主要职责：
 * 1.收取worker节点的心跳信息
 * 2.汇总集群资源情况
 * Created by bbq on 2015/11/10
 */
class Master extends Actor with Logging{

  override def preStart()={
    logInfo("Starting Master Actor")
  }
  override def receive: Receive = {
    case RegisterWorker =>
      logInfo("Receiving Worker Registing...")
      sender() ! RegisteredWorker
  }
}
object Master extends Logging{
  val SYSTEM_NAME="sparkMaster"
  val ACTOR_NAME="Master"

  def main(args: Array[String]) {
    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, "127.0.0.1", 60005)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))
    val actorRef = rpcEnv.doCreateActor(Props(classOf[Master]),ACTOR_NAME)
  }
}