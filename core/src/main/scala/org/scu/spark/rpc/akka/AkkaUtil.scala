package org.scu.spark.rpc.akka

import akka.actor.{ExtendedActorSystem, ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory

/**
 * Created by bbq on 2015/11/11
 */
case class RpcEnvConfig(name: String, host: String, port: Int)

object AkkaUtil {
  def doCreateActorSystem(config: RpcEnvConfig): ActorSystem = {
    val akkaConf = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
         |akka.remote.netty.tcp.hostname = ${config.host}
          |akka.remote.netty.tcp.port = ${config.port}
       """.stripMargin)
    ActorSystem(config.name, akkaConf)
  }

  //返回akkaActor的地址
  def generateRpcAddress(
             systemName:String,
                        rpcAddress: RpcAddress,
             actorName:String
               ):String={
    s"akka.tcp://$systemName@${rpcAddress.host}:${rpcAddress.port}/user/$actorName"
  }
  
  /**通过actorRef获取RPCAddress*/
  def getRpcAddressFromActor(actor:ActorRef):RpcAddress={
    val akkaAddress = actor.path.address
    RpcAddress(akkaAddress.host.get,akkaAddress.port.get)
  }

  /**通过actorSystem互殴RPCaddress*/
  def getRpcAddressFromSys(actorSystem: ActorSystem):RpcAddress={
    val address = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    RpcAddress(address.host.get,address.port.get)
  }
}

