package org.scu.spark.rpc.akka

import akka.actor.ActorSystem
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
  def address(
             systemName:String,
             host:String,
             port:Int,
             actorName:String
               ):String={
    s"akka.tcp://$systemName@$host:$port/user/$actorName"
  }

}

