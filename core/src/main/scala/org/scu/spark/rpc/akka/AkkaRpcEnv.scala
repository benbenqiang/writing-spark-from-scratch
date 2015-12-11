package org.scu.spark.rpc.akka

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ExtendedActorSystem, ActorRef, ActorSystem, Props}
import org.scu.spark.Logging

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}

/**
 * Created by bbq on 2015/11/11
 */
class AkkaRpcEnv(private[spark] val actorSystem: ActorSystem) extends Logging {

  /**
   * RpcEnv下所包含的actor名称对应Ref表
   */
  val actornameToRef = new ConcurrentHashMap[String,ActorRef]()

  val address : RpcAddress = {
    val defaultAddress = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    RpcAddress(defaultAddress.host.get,defaultAddress.port.get)
  }

  /**
   * 远程连接的timeout
   */
  val defaultLookupTimeout = FiniteDuration(10, SECONDS)

  /**
   * 创建actor,并将创建的actor添加到map中
   */
  def doCreateActor(props: Props, actorName: String): ActorRef = {
    val ref = actorSystem.actorOf(props, actorName)
    actornameToRef.put(actorName,ref)
    ref
  }

  /**
   * 连接远程Actor对象
   */
  def setupEndpointRef(systemName: String, host: String, port: Int, actorName: String): ActorRef = {
    val uri = AkkaUtil.address(systemName, host, port, actorName)
    val ref = Await.result(asyncSetupEndpointRefByURI(uri), defaultLookupTimeout)
    logInfo("successful created remote actor ref:" + ref)
    ref
  }

  /**
   * 异步请求远程actor
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[ActorRef] = {
    actorSystem.actorSelection(uri).resolveOne(defaultLookupTimeout)
  }
}

