package org.scu.spark.deploy.worker

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Props, Actor, ActorRef}
import org.scu.spark.Logging
import org.scu.spark.deploy.master.Master
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcAddress, RpcEnvConfig}
import org.scu.spark.deploy.DeployMessage._
/**
 * Created by bbq on 2015/11/11
 */
class Worker(rpcEnv:AkkaRpcEnv,masterRpcAddress:RpcAddress) extends Actor with Logging{
  //用于生成Woker的ID
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")



  override def preStart() ={
    logInfo("Starting Spark worker")
    tryRegisterMasters()
  }
  override def receive: Receive = {
    case RegisteredWorker=>
      logInfo(s"Register to : $masterRpcAddress connected!")
    case _ =>
      logInfo("no receive defined!")
  }

  def tryRegisterMasters()={
    logInfo("Connecting to master:"+masterRpcAddress)
    val masterEndpoint = rpcEnv.setupEndpointRef(Master.SYSTEM_NAME,masterRpcAddress.host,masterRpcAddress.port,Master.ACTOR_NAME)
    registerMaster(masterEndpoint)
  }

  def registerMaster(master:ActorRef)={
    master ! RegisterWorker
  }


  /**
   * 根据时间和host：port 生成WokerID
   * @return
   */
  private def generateWorkerId():String={
    val date = createDateFormat.format(new Date)
    s"Worker-date-"
  }
}

object Worker extends Logging{
  val SYSTEM_NAME="sparkWorker"
  val ACTOR_NAME="Worker"

  def main(args: Array[String]) {
    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, "127.0.0.1", 60006)
    val masterRpcAddress = RpcAddress("127.0.0.1",60005)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))
    val actorRef = rpcEnv.doCreateActor(Props(classOf[Worker],rpcEnv,masterRpcAddress),ACTOR_NAME)
  }
}