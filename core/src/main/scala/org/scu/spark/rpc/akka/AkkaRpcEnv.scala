package org.scu.spark.rpc.akka

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ExtendedActorSystem, ActorRef, ActorSystem, Props}
import org.scu.spark.{SparkConf, Logging}
import akka.pattern.ask
import org.scu.spark.util.RpcUtils

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

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
  def setupEndpointRef(systemName: String, rpcAddress: RpcAddress, actorName: String): ActorRef = {
    val uri = AkkaUtil.generateRpcAddress(systemName, rpcAddress, actorName)
    val ref = Await.result(asyncSetupEndpointRefByURI(uri), AkkaRpcEnv.defaultLookupTimeout)
    logInfo("successful created remote actor ref:" + ref)
    ref
  }

  /**
   * 同步连接远程Actor by url
   */
  def setupEndpointRefByURI(uri:String):ActorRef={
    val ref = Await.result(asyncSetupEndpointRefByURI(uri),AkkaRpcEnv.defaultLookupTimeout)
    logInfo("successful created remote actor ref:" + ref)
    ref
  }
  /**
   * 异步请求远程actor
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[ActorRef] = {
    val ref = actorSystem.actorSelection(uri).resolveOne(AkkaRpcEnv.defaultLookupTimeout)
    logInfo("successful created remote actor ref:" + ref)
    ref
  }


}

object AkkaRpcEnv extends  Logging{
  /**
    * 远程连接的timeout
    */
  val defaultLookupTimeout = FiniteDuration(10, SECONDS)

  /**同步请求远程对象*/
  def askSyn[T:ClassTag](actroRef:ActorRef,message:Any,conf:SparkConf):T = {
    implicit val timeout = RpcUtils.askRpcTimeout(conf)
    val ref = Await.result(actroRef.ask(message),defaultLookupTimeout).asInstanceOf[T]
    logInfo("successful get remote " +ref.getClass.getName +" object:" + ref)
    ref
  }
}

